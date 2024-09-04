#include <postgres.h>

#include <string.h>
#include <storage/bufmgr.h>
#include <access/reloptions.h>
#include <access/tableam.h>
#include <storage/indexfsm.h>
#include <commands/vacuum.h>
#include <access/generic_xlog.h>
#include <nodes/execnodes.h>
#include <utils/memutils.h>

#include "bitmap.h"

static relopt_kind bm_relopt_kind;
/*
 * Module initialize function: initialize info about bitmap relation options.
 *
 */
void _PG_init(void) {
  bm_relopt_kind = add_reloption_kind();
}

static void initBitmapState(BitmapState *state, Relation index) {
}

bytea *bmoptions(Datum reloptions, bool validate) {
  static const relopt_parse_elt tab[] = {{},{}};

  return (bytea *) build_reloptions(reloptions, validate,
                                    bm_relopt_kind,
                                    sizeof(BitmapOptions),
                                    tab, lengthof(tab));
 }

static BlockNumber bm_insert_tuple(Relation index, BlockNumber blkno, ItemPointer ctid) {
  Buffer buffer = InvalidBuffer;
  Buffer nbuffer = InvalidBuffer;
  BitmapTuple *tup = bitmap_form_tuple(ctid);
  Page page;
  BitmapPageOpaque opaque;
  BlockNumber firstBlk = blkno;
  // try insert bitmap tuple from the first block
  // because we never delete bitmap tuple, there's no possibility
  // of inserting duplicate records for a same heap block
  while (blkno != InvalidBlockNumber) {
    if (buffer != InvalidBuffer)
        UnlockReleaseBuffer(buffer);

    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buffer);

    if (bm_page_add_tup(page, tup)) {
      UnlockReleaseBuffer(buffer);
      return firstBlk;
    }

    opaque = BitmapPageGetOpaque(page);
    blkno = opaque->nextBlk;
  }

  blkno = GetFreeIndexPage(index);
  if (blkno == InvalidBlockNumber) {
    nbuffer = ReadBuffer(index, P_NEW);
    blkno = BufferGetBlockNumber(nbuffer);
  } else {
    nbuffer = ReadBuffer(index, blkno);
  }

  if (buffer != InvalidBuffer) {
    opaque = BitmapPageGetOpaque(page);
    opaque->nextBlk = blkno;
    UnlockReleaseBuffer(buffer);
  }

  LockBuffer(nbuffer, BUFFER_LOCK_EXCLUSIVE);
  page = BufferGetPage(nbuffer);
  if (!bm_page_add_tup(page, tup))
    elog(ERROR, "insert bitmap tuple failed on new page");

  opaque = BitmapPageGetOpaque(page);
  opaque->nextBlk = InvalidBlockNumber;

  UnlockReleaseBuffer(nbuffer);

  return firstBlk == InvalidBlockNumber ? blkno:firstBlk;
}

static BlockNumber bm_insert_val(Relation index, BlockNumber endblk, IndexTuple itup) {
  BitmapValPageOpaque opaque;
  Page newPage;
  Page page;
  Size sizeNeeded;
  OffsetNumber maxoff;
  BlockNumber blkno;
  Buffer buffer;

  buffer = ReadBuffer(index, endblk);
  page = BufferGetPage(buffer);
  
  sizeNeeded = IndexTupleSize(itup) + sizeof(ItemIdData);
  if (PageGetFreeSpace(page) >= sizeNeeded) {
    maxoff = PageGetMaxOffsetNumber(page) + 1;
    if (PageAddItem(page, (Item)itup, IndexTupleSize(itup), maxoff, false, false) != maxoff)
      elog(ERROR, "failed to add item to index data page");

    return endblk;
  }

  blkno = GetFreeIndexPage(index);
  if (blkno == InvalidBlockNumber) {
    buffer = ReadBuffer(index, P_NEW);
    blkno = BufferGetBlockNumber(buffer);
  } else {
    buffer = ReadBuffer(index, blkno);
  }
  opaque = BitmapValPageGetOpaque(page);
  opaque->nextBlk = blkno;

  newPage = BufferGetPage(buffer);
  PageAddItem(newPage, (Item)itup, IndexTupleSize(itup), 1, false, false);

  return blkno;
}

bool bminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
             Relation heapRel, IndexUniqueCheck checkUnique,
             bool indexUnchanged, IndexInfo *indexInfo) {
  BitmapState *bmstate = (BitmapState *) indexInfo->ii_AmCache;
  MemoryContext oldCxt;
  IndexTuple itup;
  BitmapMetaPageData *metaData;
  BlockNumber firstBlk;
  Buffer metaBuffer;
  int valIndx;

  bmstate->tmpCxt = AllocSetContextCreate(CurrentMemoryContext, "bitmap insert context", ALLOCSET_DEFAULT_SIZES);
  oldCxt = MemoryContextSwitchTo(bmstate->tmpCxt);
  
  initBitmapState(bmstate, index);

  metaBuffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
  metaData = BitmapPageGetMeta(BufferGetPage(metaBuffer));

  valIndx = bm_get_val_index(index, values, isnull);
  if (valIndx < 0) {
    if (metaData->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded on bitmap index \"%s\"",
          RelationGetRelationName(index));
    }

    LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
    LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    metaData->valBlkEnd = bm_insert_val(index, metaData->valBlkEnd, itup);
    metaData->ndistinct++;
    valIndx = bm_get_val_index(index, values, isnull);
  }

  if (valIndx >= 0) {
    firstBlk = metaData->firstBlk[valIndx];
    metaData->firstBlk[valIndx] = bm_insert_tuple(index, firstBlk, ht_ctid);
  }

  UnlockReleaseBuffer(metaBuffer);
	MemoryContextSwitchTo(oldCxt);
	MemoryContextReset(bmstate->tmpCxt);

  return false;
}

static void bmBuildCallback(Relation index, ItemPointer tid, Datum *values,
                             bool *isnull, bool tupleIsAlive, void *state) {
   BitmapBuildState *buildstate = (BitmapBuildState *) state;
   MemoryContext oldCtx;
   BitmapPageOpaque opaque;
   IndexTuple  itup;
   BitmapTuple *btup;
   Page bufpage;
   int valIdx;

   oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
   valIdx = bm_get_val_index(index, values, isnull);

  if (valIdx < 0) {
    if (buildstate->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded");
      MemoryContextSwitchTo(oldCtx);
      return;
    }

    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    buildstate->valEndBlk = bm_insert_val(index, buildstate->valEndBlk, itup);
    buildstate->ndistinct++;
  }

  if (!buildstate->blocks[valIdx]) {
    buildstate->blocks[valIdx] = (PGAlignedBlock *)palloc0(BLCKSZ);
    PageInit((Page)buildstate->blocks[valIdx], BLCKSZ, sizeof(BitmapPageSpecData));
  }

  btup = bitmap_form_tuple(tid);
  bufpage = (Page)buildstate->blocks[valIdx];

  if (!bm_page_add_tup(bufpage, btup)) {
    Page		page;
    Buffer		buffer = bm_new_buffer(index);
    GenericXLogState *state;

    opaque = BitmapPageGetOpaque(bufpage);
    // TODO: this is not correct, tmp buffer is the last page for a distinct index keys
    opaque->nextBlk = BufferGetBlockNumber(buffer);

    if (buildstate->firstBlks[valIdx] == InvalidBlockNumber)
      buildstate->firstBlks[valIdx] = opaque->nextBlk;

    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
    memcpy(page, bufpage, BLCKSZ);
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buffer);

    PageInit(bufpage, BLCKSZ, sizeof(BitmapPageSpecData));

    if (!bm_page_add_tup(bufpage, btup)) {
      elog(ERROR, "could not add new tuple to empty page");
    }
  }

  buildstate->indtuples++;
	MemoryContextSwitchTo(oldCtx);
 }

IndexBuildResult *bmbuild(Relation heap, Relation index,
                           IndexInfo *indexInfo) {
	IndexBuildResult *result;
	double		reltuples;
	BitmapBuildState buildstate;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

  /* Initialize the meta page */
	bm_init_metapage(index, MAIN_FORKNUM);

	/* Initialize the bloom build state */
	memset(&buildstate, 0, sizeof(buildstate));
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Bitmap build temporary context",
											  ALLOCSET_DEFAULT_SIZES);
  buildstate.blocks = palloc0(sizeof(PGAlignedBlock*)* MAX_DISTINCT);
  buildstate.firstBlks = palloc0(sizeof(BlockNumber) * MAX_DISTINCT);
  // set all first block number to invalid block number
  memset(buildstate.firstBlks, 0xFF, sizeof(BlockNumber) * MAX_DISTINCT);

	/* Do the heap scan */
	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   bmBuildCallback, (void *) &buildstate,
									   NULL);

	/* Flush last page if needed (it will be, unless heap was empty) */
	if (buildstate.count > 0)
		bm_flush_cached(index, &buildstate);

  // TODO: copy state fields to meta page

	MemoryContextDelete(buildstate.tmpCtx);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
 }

void bmbuildempty(Relation index) {
  bm_init_metapage(index, INIT_FORKNUM);
}


PG_FUNCTION_INFO_V1(bmhandler);

Datum bmhandler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BITMAP_NSTRATEGIES;
	amroutine->amsupport = 0;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = bmbuild;
	amroutine->ambuildempty = bmbuildempty;
	amroutine->aminsert = bminsert;
	amroutine->ambulkdelete = bmbulkdelete;
	amroutine->amvacuumcleanup = bmvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = bmcostestimate;
	amroutine->amoptions = bmoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = bmvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = bmbeginscan;
	amroutine->amrescan = bmrescan;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = bmgetbitmap;
	amroutine->amendscan = bmendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}