#include <postgres.h>

#include <funcapi.h>
#include <storage/bufmgr.h>
#include <storage/indexfsm.h>
#include <access/generic_xlog.h>
#include <access/relation.h>
#include <catalog/namespace.h>
#include <utils/varlena.h>
#include <miscadmin.h>

#include "bitmap.h"

bool bm_page_add_tup(Page page, BitmapTuple *tuple) {
  BitmapTuple *itup;
  BitmapPageOpaque opaque;
  Pointer ptr;

  opaque = BitmapPageGetOpaque(page);
  for (int i = 1; i <= opaque->maxoff; i++) { 
    itup = BitmapPageGetTuple(page, i);
    if (itup->heapblk == tuple->heapblk) {
      for (int j = 0; j < MAX_BITS_32; j++) {
        itup->bm[j] |= tuple->bm[j];
      }
      return true;
    }
  }

  if (PageGetFreeSpace(page) < sizeof(BitmapTuple))
    return false;

  opaque->maxoff++;
  ptr = (Pointer)BitmapPageGetTuple(page, opaque->maxoff);
  memcpy(ptr, (Pointer) tuple, sizeof(BitmapTuple));

	/* Adjust maxoff and pd_lower */
  ptr = (Pointer)BitmapPageGetTuple(page, opaque->maxoff + 1);
  ((PageHeader) page)->pd_lower = ptr - page;

  return true;
}

int bm_get_val_index(Relation index, Datum *values, bool *isnull) {
  Page page;
  BlockNumber blkno;
  int idx;
  Buffer buffer;
  OffsetNumber maxoff;
  BitmapPageOpaque opaque;
  
  blkno = BITMAP_VALPAGE_START_BLKNO;
  idx = 0;

  do {
    buffer = ReadBuffer(index, blkno);
    page = BufferGetPage(buffer);
    maxoff = PageGetMaxOffsetNumber(page);

    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
      ItemId		itid = PageGetItemId(page, off);
      IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, itid);
      if (bm_vals_equal(index, values, isnull, idxtuple)) {
        ReleaseBuffer(buffer);
        return idx;
      }

      idx++;
    }

    opaque = BitmapPageGetOpaque(page);
    blkno = opaque->nextBlk;
    ReleaseBuffer(buffer);
  } while (BlockNumberIsValid(blkno));

  return -1;
}

Buffer bm_newbuf_exlocked(Relation index) {
    Buffer		buffer;
	
    for (;;)
	{
		BlockNumber blkno = GetFreeIndexPage(index);
		if (blkno == InvalidBlockNumber)
			break;

		buffer = ReadBuffer(index, blkno);
		/*
		 * We have to guard against the possibility that someone else already
		 * recycled this page; the buffer may be locked if so.
		 */
		if (ConditionalLockBuffer(buffer))
		{
			Page page = BufferGetPage(buffer);
			if (PageIsNew(page))
				return buffer;

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* extend the file */
	buffer = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
							   EB_LOCK_FIRST);
    
    return buffer;
}

void bm_init_page(Page page, uint16 pgtype) {
    BitmapPageOpaque opaque;

    PageInit(page, BLCKSZ, sizeof(BitmapPageSpecData));
    opaque = BitmapPageGetOpaque(page);
    opaque->maxoff = 0;
    opaque->nextBlk = InvalidBlockNumber;
    opaque->pgtype = pgtype;
}

void bm_init_metapage(Relation index, ForkNumber fork) {
    Buffer		metabuf;
	Page		metapage;
    BitmapMetaPageData *meta;
    size_t i;

	GenericXLogState *state;

    metabuf = ReadBufferExtended(index, fork, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(metabuf) == BITMAP_METAPAGE_BLKNO);

    state = GenericXLogStart(index);
	metapage = GenericXLogRegisterBuffer(state, metabuf,
										 GENERIC_XLOG_FULL_IMAGE);

    bm_init_page(metapage, BITMAP_PAGE_META);
    meta = BitmapPageGetMeta(metapage);
    meta->magic = BITMAP_MAGIC_NUMBER;
    meta->ndistinct = 0;
    meta->valBlkEnd = InvalidBlockNumber;
    for (i = 0; i < MAX_DISTINCT; i++)
        meta->firstBlk[i] = InvalidBlockNumber;

    ((PageHeader) metapage)->pd_lower += offsetof(BitmapMetaPageData, firstBlk) + \
        sizeof(BlockNumber) * MAX_DISTINCT;

    Assert(((PageHeader) metapage)->pd_lower <= ((PageHeader) metapage)->pd_upper);

    GenericXLogFinish(state);
	UnlockReleaseBuffer(metabuf);
}

void bm_flush_cached(Relation index, BitmapBuildState *state) {
    BitmapPageOpaque opaque;
    Page page;
    Page bufpage;
    Buffer buffer;
    Buffer prevbuff;
    GenericXLogState *xlogstate;

    for (size_t i = 0; i < state->ndistinct; i++) {
        bufpage = (Page)state->blocks[i];
        opaque = BitmapPageGetOpaque(bufpage);

        if (opaque->maxoff > 0) {
            buffer = bm_newbuf_exlocked(index);

            if (state->prevBlks[i] != InvalidBlockNumber) {
                prevbuff = ReadBuffer(index, state->prevBlks[i]);
                LockBuffer(prevbuff, BUFFER_LOCK_EXCLUSIVE);
                opaque = BitmapPageGetOpaque(BufferGetPage(prevbuff));
                opaque->nextBlk = state->prevBlks[i];
                UnlockReleaseBuffer(prevbuff);
            }

            xlogstate = GenericXLogStart(index);
            page = GenericXLogRegisterBuffer(xlogstate, buffer, GENERIC_XLOG_FULL_IMAGE);
            memcpy(page, bufpage, BLCKSZ);
            GenericXLogFinish(xlogstate);
            UnlockReleaseBuffer(buffer);
        }
    }
}

PG_FUNCTION_INFO_V1(bm_metap);

/* -------------------------------------
 * Get bitmap meta page information
 * 
 * Usage: SELECT * FROM bm_metap('index_name')
 */

Datum bm_metap(PG_FUNCTION_ARGS) {
    text	   *relname = PG_GETARG_TEXT_PP(0);
	Datum		result;
	Relation	rel;
	RangeVar   *relrv;
	BitmapMetaPageData *meta;
	TupleDesc	tupleDesc;
	int			j;
	char	   *values[9];
	Buffer		buffer;
	Page		page;
	HeapTuple	tuple;
    int max_block_shown = 10;
    int i;
    StringInfoData strinfo;

    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
        	 errmsg("\"%s\" is not a %s index",
						RelationGetRelationName(rel), "bitmap")));

    buffer = ReadBuffer(rel, 0);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);
	meta = BitmapPageGetMeta(page);

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

    j = 0;
    values[j++] = psprintf("0x%X", meta->magic);
    values[j++] = psprintf("%u", meta->ndistinct);
    values[j++] = psprintf("%u", meta->valBlkEnd);

    initStringInfo(&strinfo);
    for (i = 0; i < meta->ndistinct && i < max_block_shown; i++) {
        if (i > 0) appendStringInfoString(&strinfo, ", ");
        appendStringInfoString(&strinfo, psprintf("%u", meta->firstBlk[i]));
    }
    values[j++] = strinfo.data;

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc), values);
	result = HeapTupleGetDatum(tuple);

	UnlockReleaseBuffer(buffer);
	relation_close(rel, AccessShareLock);

	PG_RETURN_DATUM(result);
}