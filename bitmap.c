#include <postgres.h>

#include <fmgr.h>
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

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

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
  
  // insert bitmap tuple from the first block
  // because we never delete bitmap tuple, there's no possibility
  // of inserting duplicate records for one heap block
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

  nbuffer = bm_newbuf_exlocked(index);
  blkno = BufferGetBlockNumber(nbuffer);

  if (buffer != InvalidBuffer) {
    opaque = BitmapPageGetOpaque(page);
    opaque->nextBlk = blkno;
    UnlockReleaseBuffer(buffer);
  }

  page = BufferGetPage(nbuffer);
  bm_init_page(page, BITMAP_PAGE_INDEX);

  if (!bm_page_add_tup(page, tup))
    elog(ERROR, "insert bitmap tuple failed on new page");

  opaque = BitmapPageGetOpaque(page);
  opaque->nextBlk = InvalidBlockNumber;

  UnlockReleaseBuffer(nbuffer);

  return firstBlk == InvalidBlockNumber ? blkno:firstBlk;
}

static BlockNumber bm_insert_val(Relation index, BlockNumber endblk, IndexTuple itup) {
  BitmapPageOpaque opaque;
  Page page;
  OffsetNumber maxoff;
  BlockNumber blkno;
  Buffer buffer;
  Buffer nbuffer;

  if (endblk == InvalidBlockNumber) {
    buffer = bm_newbuf_exlocked(index);
    blkno = BufferGetBlockNumber(buffer);
    endblk = blkno;
    Assert(blkno == BITMAP_VALPAGE_START_BLKNO);
    page = BufferGetPage(buffer);
    bm_init_page(page, BITMAP_PAGE_VALUE);
  } else {
    buffer = ReadBuffer(index, endblk);
    page = BufferGetPage(buffer);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
  }
  
  if (PageGetFreeSpace(page) >= (IndexTupleSize(itup) + sizeof(ItemIdData))) {
    maxoff = PageGetMaxOffsetNumber(page) + 1;
    if (PageAddItem(page, (Item)itup, IndexTupleSize(itup), maxoff, false, false) != maxoff)
      elog(ERROR, "failed to add item to index data page");

    UnlockReleaseBuffer(buffer);
    return endblk;
  }

  nbuffer = bm_newbuf_exlocked(index);
  blkno = BufferGetBlockNumber(nbuffer);
  opaque = BitmapPageGetOpaque(page);
  opaque->nextBlk = blkno;
  UnlockReleaseBuffer(buffer);

  page = BufferGetPage(nbuffer);
  bm_init_page(page, BITMAP_PAGE_VALUE);
  PageAddItem(page, (Item)itup, IndexTupleSize(itup), 1, false, false);
  UnlockReleaseBuffer(nbuffer);

  return blkno;
}

bool bminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
             Relation heapRel, IndexUniqueCheck checkUnique,
             bool indexUnchanged, IndexInfo *indexInfo) {
  BitmapState *bmstate = (BitmapState *) indexInfo->ii_AmCache;
  MemoryContext oldCxt;
  IndexTuple itup;
  BitmapMetaPageData *metadata;
  BlockNumber firstblk;
  Buffer metabuf;
  int valindex = -1;

  if (bmstate == NULL) {
    oldCxt = MemoryContextSwitchTo(indexInfo->ii_Context);
    bmstate = palloc0(sizeof(BitmapState));
    bmstate->tmpCxt = AllocSetContextCreate(CurrentMemoryContext, "bitmap insert context",
        ALLOCSET_DEFAULT_SIZES);
    indexInfo->ii_AmCache = (void *) bmstate;
    MemoryContextSwitchTo(oldCxt);
  }


  oldCxt = MemoryContextSwitchTo(bmstate->tmpCxt);
  initBitmapState(bmstate, index);

  metabuf = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
  metadata = BitmapPageGetMeta(BufferGetPage(metabuf));

  valindex = bm_get_val_index(index, values, isnull);
  if (valindex < 0) {
    if (metadata->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded on bitmap index \"%s\"",
          RelationGetRelationName(index));
    }

    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    metadata->valBlkEnd = bm_insert_val(index, metadata->valBlkEnd, itup);
    valindex = metadata->ndistinct++;
  }

  if (valindex >= 0) {
    firstblk = metadata->firstBlk[valindex];
    metadata->firstBlk[valindex] = bm_insert_tuple(index, firstblk, ht_ctid);
  }

  UnlockReleaseBuffer(metabuf);
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
  BlockNumber blkno;
  Page bufpage;
  Page page;
  Buffer buffer;
  Buffer pbuffer;
  GenericXLogState *xlogstate;
  int valindex = -1;

  oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

  if (buildstate->ndistinct > 0) {
    valindex = bm_get_val_index(index, values, isnull);
  }

  if (valindex < 0) {
    if (buildstate->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded");
      MemoryContextSwitchTo(oldCtx);
      return;
    }

    // TODO (performance): put all distinctive values in build state,
    // at the end of building, materialize to block page
    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    buildstate->valEndBlk = bm_insert_val(index, buildstate->valEndBlk, itup);
    buildstate->ndistinct++;
    valindex = bm_get_val_index(index, values, isnull);
    Assert(valindex >= 0);
  }

  if (!buildstate->blocks[valindex]) {
    buildstate->blocks[valindex] = (PGAlignedBlock *)palloc0(BLCKSZ);
    bm_init_page((Page)buildstate->blocks[valindex], BITMAP_PAGE_INDEX);
  }

  btup = bitmap_form_tuple(tid);
  bufpage = (Page)buildstate->blocks[valindex];

  if (!bm_page_add_tup(bufpage, btup)) {
    buffer = bm_newbuf_exlocked(index);
    blkno = BufferGetBlockNumber(buffer);

    if (buildstate->firstBlks[valindex] == InvalidBlockNumber)
      buildstate->firstBlks[valindex] = blkno;

    // set next block number in previous block page
    if (buildstate->prevBlks[valindex] != InvalidBlockNumber) {
      pbuffer = ReadBuffer(index, buildstate->prevBlks[valindex]);
      LockBuffer(pbuffer, BUFFER_LOCK_EXCLUSIVE);
      opaque = BitmapPageGetOpaque(BufferGetPage(pbuffer));
      opaque->nextBlk = blkno;
      UnlockReleaseBuffer(pbuffer);
    } else {
      buildstate->prevBlks[valindex] = blkno;
    }

    xlogstate = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(xlogstate, buffer, GENERIC_XLOG_FULL_IMAGE);
    memcpy(page, bufpage, BLCKSZ);
    GenericXLogFinish(xlogstate);
    UnlockReleaseBuffer(buffer);

    bm_init_page(bufpage, BITMAP_PAGE_INDEX);
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
  Buffer buffer;
  Page metapage;
  BitmapMetaPageData *metadata;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

  /* Initialize the meta page */
	bm_init_metapage(index, MAIN_FORKNUM);
  buffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
  LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
  metapage = BufferGetPage(buffer);
  metadata = BitmapPageGetMeta(metapage);

	/* Initialize the build state */
	memset(&buildstate, 0, sizeof(buildstate));
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Bitmap build temporary context",
											  ALLOCSET_DEFAULT_SIZES);
  buildstate.valEndBlk = InvalidBlockNumber;
  buildstate.blocks = palloc0(sizeof(PGAlignedBlock*)* MAX_DISTINCT);
  buildstate.firstBlks = palloc0(sizeof(BlockNumber) * MAX_DISTINCT);
  buildstate.prevBlks = palloc0(sizeof(BlockNumber) * MAX_DISTINCT);
  memset(buildstate.firstBlks, 0xFF, sizeof(BlockNumber) * MAX_DISTINCT);
  memset(buildstate.prevBlks, 0xFF, sizeof(BlockNumber) * MAX_DISTINCT);

	/* Do the heap scan */
	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   bmBuildCallback, (void *) &buildstate,
									   NULL);

	bm_flush_cached(index, &buildstate);

  metadata->valBlkEnd = buildstate.valEndBlk;
  metadata->ndistinct = buildstate.ndistinct;
  memcpy(metadata->firstBlk, buildstate.firstBlks, sizeof(BlockNumber) * buildstate.ndistinct);
  UnlockReleaseBuffer(buffer);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	MemoryContextDelete(buildstate.tmpCtx);

	return result;
 }

void bmbuildempty(Relation index) {
  bm_init_metapage(index, INIT_FORKNUM);
}


PG_FUNCTION_INFO_V1(bmhandler);

Datum bmhandler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BITMAP_NSTRATEGIES;
	amroutine->amsupport = 1;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = false;
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
	amroutine->amgettuple = bmgettuple;
	amroutine->amgetbitmap = bmgetbitmap;
	amroutine->amendscan = bmendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}