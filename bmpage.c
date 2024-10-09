#include <postgres.h>

#include <storage/bufmgr.h>
#include <storage/indexfsm.h>
#include <storage/lmgr.h>
#include <access/generic_xlog.h>

#include "bitmap.h"

bool
bm_page_add_tup(Page page, BitmapTuple * tuple)
{
	BitmapTuple *itup;
	BitmapPageOpaque opaque;
	Pointer		ptr;

	opaque = BitmapPageGetOpaque(page);
	for (int i = 1; i <= opaque->maxoff; i++)
	{
		itup = BitmapPageGetTuple(page, i);
		if (itup->heapblk == tuple->heapblk)
		{
			for (int j = 0; j < MAX_BITS_32; j++)
			{
				itup->bm[j] |= tuple->bm[j];
			}
			return true;
		}
	}

	if (PageGetFreeSpace(page) < sizeof(BitmapTuple))
		return false;

	opaque->maxoff++;
	ptr = (Pointer) BitmapPageGetTuple(page, opaque->maxoff);
	memcpy(ptr, (Pointer) tuple, sizeof(BitmapTuple));

	/* Adjust maxoff and pd_lower */
	ptr = (Pointer) BitmapPageGetTuple(page, opaque->maxoff + 1);
	((PageHeader) page)->pd_lower = ptr - page;

	return true;
}

BitmapMetaPageData *
bm_get_meta(Relation index)
{
	Buffer		buffer;
	BitmapMetaPageData *meta,
			   *metacpy;
	int			size;

	buffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	meta = BitmapPageGetMeta(BufferGetPage(buffer));
	size = offsetof(BitmapMetaPageData, startBlk) + sizeof(BlockNumber) * MAX_DISTINCT;
	metacpy = palloc0(size);
	memcpy(metacpy, meta, size);
	UnlockReleaseBuffer(buffer);

	return metacpy;
}

/* insert index key values into value page, starts from block 1
   index values are never deleted onced inserted, so the pages only keep extending */
int
bm_insert_val(Relation index, Datum *values, bool *isnull)
{
	Page		page;
	OffsetNumber maxoff,
				off;
	BlockNumber blkno;
	Buffer		buffer;
	Buffer		nbuffer;
	BitmapPageOpaque opaque;
	IndexTuple	itup;
	int			valIndex = 0;
	GenericXLogState *gxstate;

start:
	blkno = BITMAP_VALPAGE_START_BLKNO;
	for (;;)
	{
		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		maxoff = PageGetMaxOffsetNumber(page);
		opaque = BitmapPageGetOpaque(page);

		Assert(opaque->pgtype == BITMAP_PAGE_VALUE);

		for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		itid = PageGetItemId(page, off);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, itid);

			if (bm_vals_equal(index, values, isnull, idxtuple))
			{
				UnlockReleaseBuffer(buffer);
				return valIndex;
			}

			valIndex++;
		}

		blkno = opaque->nextBlk;
		if (blkno == InvalidBlockNumber)
		{
			break;
		}

		UnlockReleaseBuffer(buffer);
	}

	if (valIndex == MAX_DISTINCT - 1)
		elog(WARNING, "max distinct exceeded on bitmap index \"%s\"",
			 RelationGetRelationName(index));


	gxstate = GenericXLogStart(index);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = GenericXLogRegisterBuffer(gxstate, buffer, 0);

	/*
	 * contention: new values has been inserted into the page after we upgrade
	 * to exclusive lock
	 */
	if (PageGetMaxOffsetNumber(page) != maxoff)
	{
		valIndex = 0;
		GenericXLogAbort(gxstate);
		UnlockReleaseBuffer(buffer);
		goto start;
	}

	itup = index_form_tuple(RelationGetDescr(index), values, isnull);
	if (PageGetFreeSpace(page) >= (IndexTupleSize(itup) + sizeof(ItemIdData)))
	{
		maxoff = PageGetMaxOffsetNumber(page) + 1;
		if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), maxoff, false, false) != maxoff)
			elog(ERROR, "failed to add item to index data page");

		GenericXLogFinish(gxstate);
		UnlockReleaseBuffer(buffer);
		return valIndex;
	}

	nbuffer = bm_newbuffer_locked(index);
	BitmapPageGetOpaque(page)->nextBlk = BufferGetBlockNumber(nbuffer);
	page = GenericXLogRegisterBuffer(gxstate, nbuffer, GENERIC_XLOG_FULL_IMAGE);
	bm_init_page(page, BITMAP_PAGE_VALUE);

	if (PageAddItem(page, (Item) itup, IndexTupleSize(itup), 1, false, false) != FirstOffsetNumber)
		elog(ERROR, "fail to add index value");

	GenericXLogFinish(gxstate);
	UnlockReleaseBuffer(buffer);
	UnlockReleaseBuffer(nbuffer);

	return valIndex;
}

int
bm_get_val_index(Relation index, Datum *values, bool *isnull)
{
	BlockNumber blkno = BITMAP_VALPAGE_START_BLKNO;
	int			idx = 0;
	Buffer		buffer;
	Page		page;
	OffsetNumber maxoff;
	BitmapPageOpaque opaque;

	while (BlockNumberIsValid(blkno))
	{
		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		maxoff = PageGetMaxOffsetNumber(page);

		for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		itid = PageGetItemId(page, off);
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, itid);

			if (bm_vals_equal(index, values, isnull, idxtuple))
			{
				UnlockReleaseBuffer(buffer);
				return idx;
			}

			idx++;
		}

		opaque = BitmapPageGetOpaque(page);
		blkno = opaque->nextBlk;
		UnlockReleaseBuffer(buffer);
	}

	return -1;
}

Buffer
bm_newbuffer_locked(Relation index)
{
	Buffer		buffer;
	Page		page;

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
			page = BufferGetPage(buffer);

			if (BitmapPageDeleted(page))
				return buffer;

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* extend the file */
	LockRelationForExtension(index, ExclusiveLock);
	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	UnlockRelationForExtension(index, ExclusiveLock);

	return buffer;
}

void
bm_init_page(Page page, uint16 pgtype)
{
	BitmapPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(BitmapPageSpecData));
	opaque = BitmapPageGetOpaque(page);
	opaque->maxoff = 0;
	opaque->nextBlk = InvalidBlockNumber;
	opaque->flags &= ~BITMAP_PAGE_DELETED;
	opaque->pgtype = pgtype;
}

void
bm_init_metapage(Relation index, ForkNumber fork)
{
	Buffer		metabuf;
	Page		metapage;
	BitmapMetaPageData *meta;
	size_t		i;

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
	for (i = 0; i < MAX_DISTINCT; i++)
		meta->startBlk[i] = InvalidBlockNumber;

	((PageHeader) metapage)->pd_lower += offsetof(BitmapMetaPageData, startBlk) + \
		sizeof(BlockNumber) * MAX_DISTINCT;

	Assert(((PageHeader) metapage)->pd_lower <= ((PageHeader) metapage)->pd_upper);

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metabuf);
}

void
bm_init_valuepage(Relation index, ForkNumber fork)
{
	Buffer		buffer;
	Page		page;
	BitmapPageOpaque opaque;
	GenericXLogState *state;

	buffer = ReadBufferExtended(index, fork, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(buffer) == BITMAP_VALPAGE_START_BLKNO);

	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buffer,
									 GENERIC_XLOG_FULL_IMAGE);

	bm_init_page(page, BITMAP_PAGE_VALUE);
	opaque = BitmapPageGetOpaque(page);
	opaque->maxoff = 0;
	opaque->nextBlk = InvalidBlockNumber;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
}

void
bm_flush_cached(Relation index, BitmapBuildState * state)
{
	BitmapPageOpaque opaque;
	Page		page,
				prepage,
				bufpage;
	Buffer		buffer,
				prevbuff = InvalidBuffer;
	GenericXLogState *xlogstate;

	for (size_t i = 0; i < state->ndistinct; i++)
	{
		bufpage = (Page) state->blocks[i];
		opaque = BitmapPageGetOpaque(bufpage);

		if (opaque->maxoff > 0)
		{
			buffer = bm_newbuffer_locked(index);
			xlogstate = GenericXLogStart(index);

			if (state->prevBlks[i] != InvalidBlockNumber)
			{
				prevbuff = ReadBuffer(index, state->prevBlks[i]);
				LockBuffer(prevbuff, BUFFER_LOCK_EXCLUSIVE);
				prepage = GenericXLogRegisterBuffer(xlogstate, prevbuff, 0);

				BitmapPageGetOpaque(prepage)->nextBlk = BufferGetBlockNumber(buffer);
			}

			if (state->startBlks[i] == InvalidBlockNumber)
				state->startBlks[i] = BufferGetBlockNumber(buffer);

			page = GenericXLogRegisterBuffer(xlogstate, buffer, GENERIC_XLOG_FULL_IMAGE);
			memcpy(page, bufpage, BLCKSZ);
			GenericXLogFinish(xlogstate);

			if (prevbuff != InvalidBuffer)
				UnlockReleaseBuffer(prevbuff);

			UnlockReleaseBuffer(buffer);
		}
	}
}
