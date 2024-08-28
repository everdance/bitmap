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
  Size sizeNeeded = IndexTupleSize(itup) + sizeof(ItemIdData);

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

  newPage = BufferGetPage(buffer);
  PageAddItem(newPage, (Item)itup, IndexTupleSize(tup), 1, false, false);
}
