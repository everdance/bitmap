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

BlockNumber
bm_get_firstblk(Relation index, int valIdx)
{
	BitmapMetaPageData *meta = bm_get_meta(index);

	Assert(valIdx >= 0);
	Assert(meta->magic == BITMAP_MAGIC_NUMBER);
	Assert(meta->ndistinct > valIdx);

	return meta->firstBlk[valIdx];
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
	size = offsetof(BitmapMetaPageData, firstBlk) + sizeof(BlockNumber) * meta->ndistinct;
	metacpy = palloc0(size);
	memcpy(metacpy, meta, size);
	UnlockReleaseBuffer(buffer);

	return metacpy;
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

		Assert(maxoff > 0);

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
bm_newbuf_exlocked(Relation index)
{
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
			Page		page = BufferGetPage(buffer);

			if (PageIsNew(page) || BitmapPageDeleted(page))
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
	meta->valBlkEnd = InvalidBlockNumber;
	for (i = 0; i < MAX_DISTINCT; i++)
		meta->firstBlk[i] = InvalidBlockNumber;

	((PageHeader) metapage)->pd_lower += offsetof(BitmapMetaPageData, firstBlk) + \
		sizeof(BlockNumber) * MAX_DISTINCT;

	Assert(((PageHeader) metapage)->pd_lower <= ((PageHeader) metapage)->pd_upper);

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metabuf);
}

void
bm_flush_cached(Relation index, BitmapBuildState * state)
{
	BitmapPageOpaque opaque;
	Page		page;
	Page		bufpage;
	Buffer		buffer;
	Buffer		prevbuff;
	GenericXLogState *xlogstate;

	for (size_t i = 0; i < state->ndistinct; i++)
	{
		bufpage = (Page) state->blocks[i];
		opaque = BitmapPageGetOpaque(bufpage);

		if (opaque->maxoff > 0)
		{
			buffer = bm_newbuf_exlocked(index);

			if (state->prevBlks[i] != InvalidBlockNumber)
			{
				prevbuff = ReadBuffer(index, state->prevBlks[i]);
				LockBuffer(prevbuff, BUFFER_LOCK_EXCLUSIVE);
				opaque = BitmapPageGetOpaque(BufferGetPage(prevbuff));
				opaque->nextBlk = state->prevBlks[i];
				UnlockReleaseBuffer(prevbuff);
			}

			if (state->firstBlks[i] == InvalidBlockNumber)
				state->firstBlks[i] = BufferGetBlockNumber(buffer);

			xlogstate = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(xlogstate, buffer, GENERIC_XLOG_FULL_IMAGE);
			memcpy(page, bufpage, BLCKSZ);
			GenericXLogFinish(xlogstate);
			UnlockReleaseBuffer(buffer);
		}
	}
}
