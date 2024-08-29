#include <postgres.h>
#include <storage/bufmgr.h>
#include <string.h>

#include "bitmap.h"

static relopt_kind bm_relopt_kind;
/*
 * Module initialize function: initialize info about bitmap relation options.
 *
 */
void _PG_init(void) {
  bm_relopt_kind = add_reloption_kind();
}


static BitmapOptions *makeDefaultBitmapOptions(void) {}

Datum bmhandler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	/* amroutine->amstrategies = BLOOM_NSTRATEGIES; */
	/* amroutine->amsupport = BITMAP_NPROC; */
	/* amroutine->amoptsprocnum = BLOOM_OPTIONS_PROC; */
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

void initBitmapState(BitmapState *state, Relation index) {
  state->ncols = index->rd_att->nattrs;
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
   
 }

IndexBuildResult *
bmbuild(Relation heap, Relation index, IndexInfo *indexInfo) {}

void bmbuildempty(Relation index) {
  Page metapage;
  
  metapage = (PAGE) palloc(BLKSZ);
  BitmapFillMeta(index, metapage);

  PageSetCheckSumInplace(metapage, BITMAP_METAPAGE_BLKNO);
  smgrwrite(RelationGetSmgr(index), INIT_FORKNUM, BLOOM_METAPAGE_BLKNO,
			  (char *) metapage, true);
	log_newpage(&(RelationGetSmgr(index))->smgr_rnode.node, INIT_FORKNUM,
				BLOOM_METAPAGE_BLKNO, metapage, true);
  smgrimmedsync(RelationGetSmgr(index), INIT_FORKNUM);
}

bool bminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
             Relation heapRel, IndexUniqueCheck checkUnique,
             bool indexUnchanged, IndexInfo *indexInfo) {
  BitmapState bmstate;
  BitmapTuple *bmtup;
  MemoryContext oldCtx;
  MemoryContext insertCtx;
  IndexTuple itup;
  BitmapMetaPageData *metaData;
  Buffer buffer, metaBuffer;
  Page page, metaPage;
  BlockNumber blkno = InvalidBlockNumber;

  insertCtx = AllocSetContextCreate(CurrentMemoryContext, "bitmap insert context", ALLOCSET_DEFAULT_SIZES);
  initBitmapState(&bmstate, index);

  metaBuffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
  metaData = BitmapPageGetMeta(BufferGetPage(metaBuffer));
  // load data pages to check if itup already exists and return its index number
  // otherwise insert itup as a distinct value, and increase meta data ndistinct
  valPageBlk = BITMAP_VALPAGE_START_BLKNO;
  distinctIdx = 0;
  for (;BlockNumberIsValid(valPageBlk);) {
    ReleaseBuffer(valBuffer);
    valBuffer = ReadBuffer(index, valPageBlk);
    valPage = BufferGetPage(valBuffer);
    maxoff = PageGetMaxOffsetNumber(valPage);
    for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
      ItemId		iid = PageGetItemId(page, off);
      IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
      if (is_vals_equal(values, idxtuple)) {
        found = true;
        break;
      }
      distinctIdx++;
    }

    if (found) break;
    
    valPageOpaque = BitmapValPageGetOpaque(valPage);
    valPageBlk = valPageOpaque->nextBlk;
  }

  if (!found) {
    if (metaData->ndistinct == MAX_DISTINCT) {
      elog(WARNING, "max distinct exceeded");
      return false;
    }

    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    _bm_insert_distinct(valPage, itup);
    metaData->ndistinct++;
  }
  
  _bm_upsert_ctid(metaData[distinctIdx], ht_ctid);

  return true;
}

void _bm_insert_distinct(Page page, IndexTuple itup) {
  BitmapValPageOpaque opaque;
  Page newPage;
  Size sizeNeeded;
  uint32 maxoff;
  BlockNumber blkno;
  Buffer buffer;

  sizeNeeded = IndexTupleSize(itup) + sizeof(ItemIdData);
  if (PageGetFreeSpace(page) >= sizeNeeded) {
    maxoff = PageGetMaxOffsetNumber(page) + 1;
    if (PageAddItem(page, (Item)itup, IndexTupleSize(tup), off, false, false) != off) {
      elog(ERROR, "failed to add item to index data page in \"%s\"",
						 RelationGetRelationName(index));
    }
    return;
  }

  BlockNumber blkno = GetFreeIndexPage(index);
  if (blkno == InvalidBlockNumber) {
    buffer = ReadBuffer(index, P_NEW);
    blkno = BufferGetBlockNumber(buffer);
  } else {
    buffer = ReadBuffer(index, blkno);
  }
  opaque = BitmapValPageGetOpaque(page);
  opaque->nextBlk = blkno;

  newPage = BufferGetPage(buffer);
  PageAddItem(newPage, (Item)itup, IndexTupleSize(tup), 1, false, false);
}

void _bm_upsert_ctid(BlockNumber blkno, ItemPointer ctid) {
  Buffer buffer;
  Buffer nbuffer;
  Page page;
  BitmapPageOpaque opaque;
  
  while (blkno != InvalidBlockNumber) {
    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    if (_bm_page_add_item(page, ctid)) {
      UnlockReleaseBuffer(buffer);
      return;
    }

    opaque = BitmapPageGetOpaque(page);
    blkno = opaque->nextBlk;
    UnlockReleaseBuffer(buffer);
  }

  opaque = BitmapPageGetOpaque(page);
  blkno = GetFreeIndexPage(index);
  if (blkno == InvalidBlockNumber) {
    nbuffer = ReadBuffer(index, P_NEW);
    blkno = BufferGetBlockNumber(buffer);
  } else {
    nbuffer = ReadBuffer(index, blkno);
  }
  opaque->nextBlk = blkno;
  UnlockReleaseBuffer(buffer);

  LockBuffer(nbuffer, BUFFER_LOCK_SHARE);
  page = BufferGetPage(nbuffer);
  _bm_page_add_item(page, _bm_form_tuple(ctid));  
  UnlockReleaseBuffer(nbuffer);
}

bool _bm_page_add_item(Page page, BitmapTuple *tuple) {
  BitmapTuple *itup;
  BitmapPageOpaque opaque;
  Pointer ptr;

  opaque = BitmapPageGetOpaque(page);
  for (int i = 1; i <= opaque->maxoff; i++) { 
    itup = BitmapPageGetTuple(page, i);
    if (itup->heapblk == tuple->heapblk) {
      for (int j = 0; j < MAX_BITS_32; j++) {
        itup->bm[j] &= tuple->bm[j];
      }
      return true;
    }
  }

  if (BitmapGetFreeSpace(page) < sizeof(BitmapTuple))
    return false;

  itup = BitmapPageGetTuple(page, opaque->maxoff + 1);
	memcpy((Pointer) itup, (Pointer) tuple, sizeof(BitmapTuple));

	/* Adjust maxoff and pd_lower */
	opaque->maxoff++;
	ptr = (Pointer) BitmapPageGetTuple(page, opaque->maxoff + 1);
	((PageHeader) page)->pd_lower = ptr - page;

  return true;
}

static BitmapTuple *_bitmap_form_tuple(ItemPointer ctid) {
  BitmapTuple *tuple = palloc0(sizeof(BitmapTuple));
  tuple->heapblk = BlockIdGetBlockNumber(ctid->ip_blkid);
  tuple->bm[ctid->ip_posid/32] &= 0x1 << (ctid->ip_posid%32);
  
  return tuple;
}
