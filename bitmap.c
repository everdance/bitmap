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
#include <utils/datum.h>

#include "bitmap.h"

static relopt_kind bm_relopt_kind;
/*
 * Module initialize function: initialize info about bitmap relation options.
 *
 */
void _PG_init(void) {
  bm_relopt_kind = add_reloption_kind();
}

IndexBuildResult *bmbuild(Relation heap, Relation index,
                           IndexInfo *indexInfo);
void bmbuildempty(Relation index);
bool bminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
             Relation heapRel, IndexUniqueCheck checkUnique,
             bool indexUnchanged, IndexInfo *indexInfo);
bytea *bmoptions(Datum reloptions, bool validate);
BlockNumber _bm_insert_distinct(Relation index, BlockNumber endblk, IndexTuple itup);
void _bm_upsert_ctid(Relation index, BlockNumber blkno, ItemPointer ctid);
static BitmapTuple *_bitmap_form_tuple(ItemPointer ctid);
bool _bm_vals_equal(Relation index, Datum *cmpVals, bool *cmpIsnull, IndexTuple itup);
int bm_find_val_index(Relation index, Datum *values, bool *isnull);

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

static void initBitmapState(BitmapState *state, Relation index) {
}

bytea *bmoptions(Datum reloptions, bool validate) {
  static const relopt_parse_elt tab[] = {{},{}};

  return (bytea *) build_reloptions(reloptions, validate,
                                    bm_relopt_kind,
                                    sizeof(BitmapOptions),
                                    tab, lengthof(tab));
 }

 static void bmBuildCallback(Relation index, ItemPointer tid, Datum *values,
                             bool *isnull, bool tupleIsAlive, void *state) {
   BitmapBuildState *buildstate = (BitmapBuildState *) state;
   MemoryContext oldCtx;
   IndexTuple  itup;
   BitmapTuple *btup;
   Page bufpage;
   int valIdx;

   oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
   valIdx = bm_find_val_index(index, values, isnull);

  if (valIdx < 0) {
    if (buildstate->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded");
      MemoryContextSwitchTo(oldCtx);
      return;
    }

    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    buildstate->valEndBlk = _bm_insert_distinct(index, buildstate->valEndBlk, itup);
    buildstate->ndistinct++;
  }

  if (!buildstate->blocks[valIdx]) {
    buildstate->blocks[valIdx] = (PGAlignedBlock *)palloc0(BLCKSZ);
    PageInit((Page)buildstate->blocks[valIdx], BLCKSZ, sizeof(BitmapValPageOpaque));
  }

  btup = _bitmap_form_tuple(tid);
  bufpage = (Page)buildstate->blocks[valIdx];
  if (!bm_page_add_item(bufpage, btup)) {
    Page		page;
    Buffer		buffer = bm_new_buffer(index);
    GenericXLogState *state;

    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
    memcpy(page, bufpage, BLCKSZ);
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buffer);
    PageInit(bufpage, BLCKSZ, sizeof(BitmapValPageOpaque));

    if (!bm_page_add_item(bufpage, btup)) {
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
	bm_init_metapage(index);

	/* Initialize the bloom build state */
	memset(&buildstate, 0, sizeof(buildstate));
	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											  "Bitmap build temporary context",
											  ALLOCSET_DEFAULT_SIZES);
  buildstate.blocks = palloc0(sizeof(PGAlignedBlock*)* MAX_DISTINCT);

	/* Do the heap scan */
	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   bmBuildCallback, (void *) &buildstate,
									   NULL);

	/* Flush last page if needed (it will be, unless heap was empty) */
	if (buildstate.count > 0)
		bm_flush_cached(index, &buildstate);

	MemoryContextDelete(buildstate.tmpCtx);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
 }

void bmbuildempty(Relation index) {
  Page metapage;
  
  metapage = (Page) palloc(BLCKSZ);
  bm_fill_metapage(index, metapage);

  PageSetChecksumInplace(metapage, BITMAP_METAPAGE_BLKNO);
  smgrwrite(RelationGetSmgr(index), INIT_FORKNUM, BITMAP_METAPAGE_BLKNO,
			  (char *) metapage, true);
	log_newpage(&(RelationGetSmgr(index))->smgr_rlocator.locator, INIT_FORKNUM,
				BITMAP_METAPAGE_BLKNO, metapage, true);
  smgrimmedsync(RelationGetSmgr(index), INIT_FORKNUM);
}

bool bminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
             Relation heapRel, IndexUniqueCheck checkUnique,
             bool indexUnchanged, IndexInfo *indexInfo) {
  BitmapState *bmstate = (BitmapState *) indexInfo->ii_AmCache;
  MemoryContext oldCxt;
  IndexTuple itup;
  BitmapMetaPageData *metaData;
  Buffer metaBuffer;
  int valIdx;

  bmstate->tmpCxt = AllocSetContextCreate(CurrentMemoryContext, "bitmap insert context", ALLOCSET_DEFAULT_SIZES);
  oldCxt = MemoryContextSwitchTo(bmstate->tmpCxt);
  
  initBitmapState(bmstate, index);

  metaBuffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
  metaData = BitmapPageGetMeta(BufferGetPage(metaBuffer));

  valIdx = bm_find_val_index(index, values, isnull);
  if (valIdx < 0) {
    if (metaData->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded");
      return false;
    }

    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    metaData->valBlkEnd = _bm_insert_distinct(index, metaData->valBlkEnd, itup);
    metaData->ndistinct++;
  }
  
  _bm_upsert_ctid(index, metaData->firstBlk[valIdx], ht_ctid);

  /* clenup */
	MemoryContextSwitchTo(oldCxt);
	MemoryContextReset(bmstate->tmpCxt);

  return false;
}

int bm_find_val_index(Relation index, Datum *values, bool *isnull) {
  Page page;
  BlockNumber blkno;
  int idx;
  Buffer buffer;
  OffsetNumber maxoff;
  BitmapValPageOpaque opaque;
  
  blkno = BITMAP_VALPAGE_START_BLKNO;
  idx = 0;

  do {
    buffer = ReadBuffer(index, blkno);
    page = BufferGetPage(buffer);
    maxoff = PageGetMaxOffsetNumber(page);

    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
      ItemId		itid = PageGetItemId(page, off);
      IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, itid);
      if (_bm_vals_equal(index, values, isnull, idxtuple)) {
        return idx;
      }

      idx++;
    }

    opaque = BitmapValPageGetOpaque(page);
    blkno = opaque->nextBlk;
    ReleaseBuffer(buffer);
  } while (BlockNumberIsValid(blkno));

  return -1;
}

BlockNumber _bm_insert_distinct(Relation index, BlockNumber endblk, IndexTuple itup) {
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
    if (PageAddItem(page, (Item)itup, IndexTupleSize(itup), maxoff, false, false) != maxoff) {
      elog(ERROR, "failed to add item to index data page");
    }
    // TODO increase max off in page
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

void _bm_upsert_ctid(Relation index, BlockNumber blkno, ItemPointer ctid) {
  Buffer buffer = InvalidBuffer;
  Buffer nbuffer = InvalidBuffer;
  Page page;
  BitmapPageOpaque opaque;
  
  do {
    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    if (bm_page_add_item(page, _bitmap_form_tuple(ctid))) {
      UnlockReleaseBuffer(buffer);
      return;
    }

    opaque = BitmapPageGetOpaque(page);
    blkno = opaque->nextBlk;
    UnlockReleaseBuffer(buffer);
  } while (blkno != InvalidBlockNumber);

  opaque = BitmapPageGetOpaque(page);
  blkno = GetFreeIndexPage(index);
  if (blkno == InvalidBlockNumber) {
    nbuffer = ReadBuffer(index, P_NEW);
    blkno = BufferGetBlockNumber(nbuffer);
  } else {
    nbuffer = ReadBuffer(index, blkno);
  }
  opaque->nextBlk = blkno;

  LockBuffer(nbuffer, BUFFER_LOCK_SHARE);
  page = BufferGetPage(nbuffer);
  bm_page_add_item(page, _bitmap_form_tuple(ctid));  
  UnlockReleaseBuffer(nbuffer);
}


static BitmapTuple *_bitmap_form_tuple(ItemPointer ctid) {
  BitmapTuple *tuple = palloc0(sizeof(BitmapTuple));
  tuple->heapblk = BlockIdGetBlockNumber(&ctid->ip_blkid);
  tuple->bm[ctid->ip_posid/32] &= 0x1 << (ctid->ip_posid%32);
  
  return tuple;
}

bool _bm_vals_equal(Relation index, Datum *cmpVals, bool *cmpIsnull, IndexTuple itup) {
  Datum		values[INDEX_MAX_KEYS];
  bool		isnull[INDEX_MAX_KEYS];
  TupleDesc tupDesc =  RelationGetDescr(index);
  int nattrs = tupDesc->natts;

  index_deform_tuple(itup, tupDesc, values, isnull);

  for  (int i = 0; i < nattrs; i++) {
    Form_pg_attribute att = TupleDescAttr(tupDesc, i);

    if (isnull[i] != cmpIsnull[i])
      return false;

    if (isnull[i]) continue;

    if (!datumIsEqual(cmpVals[i], values[i], att->attbyval, att->attlen))
      return false;
  }

  return true;
}
