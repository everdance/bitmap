#include <postgres.h>

#include <storage/bufmgr.h>
#include <access/generic_xlog.h>
#include <commands/vacuum.h>
#include <storage/indexfsm.h>

#include "bitmap.h"

IndexBulkDeleteResult *
bmbulkdelete(IndexVacuumInfo *info,
			 IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback,
			 void *callback_state)
{
	Relation	index = info->index;
	Buffer		buffer;
	BlockNumber blkno;
	BitmapState state;
	BitmapMetaPageData *meta;
	Page		page;
	GenericXLogState *gxlogState;
	BitmapPageOpaque opaque;
	ItemPointer tids = palloc0(sizeof(ItemPointerData) * MAX_HEAP_TUPLE_PER_PAGE);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	buffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	meta = BitmapPageGetMeta(BufferGetPage(buffer));
	blkno = meta->valBlkEnd;

	state.ndistinct = meta->ndistinct;
	if (state.ndistinct == 0)
	{
		UnlockReleaseBuffer(buffer);
		return stats;
	}

	state.blocks = palloc(sizeof(BlockNumber) * state.ndistinct);
	memcpy(state.blocks, meta->firstBlk, sizeof(BlockNumber) * state.ndistinct);
	UnlockReleaseBuffer(buffer);

	for (int i = 0; i < state.ndistinct; i++)
	{
		blkno = state.blocks[i];
		while (blkno != InvalidBlockNumber)
		{
			BitmapTuple *itup,
					   *itupPtr,
					   *itupEnd;
			OffsetNumber maxoff;

			vacuum_delay_point();

			buffer = ReadBuffer(index, blkno);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			gxlogState = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);
			opaque = BitmapPageGetOpaque(page);
			maxoff = opaque->maxoff;

			itup = itupPtr = BitmapPageGetTuple(page, FirstOffsetNumber);
			itupEnd = BitmapPageGetTuple(page, OffsetNumberNext(maxoff));

			while (itup < itupEnd)
			{
				int			count = 0;
				int			delCount = 0;

				count = bm_tuple_to_tids(itup, tids);
				for (int n = 0; n < count; n++)
				{
					if (callback(&tids[n], callback_state))
					{
						int			idx = tids[n].ip_posid - 1;

						itup->bm[idx / 32] &= ~(0x1 << (idx % 32));
						delCount++;
						stats->tuples_removed += 1;
					}
				}

				/* all tuples deleted in a single index tuple, del index tuple */
				if (delCount == count)
				{
					opaque->maxoff--;
				}
				else
				{
					if (itupPtr != itup)
						memmove((Pointer) itupPtr, (Pointer) itup, sizeof(BitmapTuple));
					itupPtr++;
				}
				itup++;
			}

			Assert(itupPtr == BitmapPageGetTuple(page, opaque->maxoff));

			if (itupPtr != itup)
			{
				if (opaque->maxoff == 0)
				{
					BitmapPageSetDeleted(page);
				}
				((PageHeader) page)->pd_lower = (Pointer) itupPtr - page;
				GenericXLogFinish(gxlogState);
			}
			else
			{
				GenericXLogAbort(gxlogState);
			}

			blkno = opaque->nextBlk;
			UnlockReleaseBuffer(buffer);
		}
	}

	return stats;
}


IndexBulkDeleteResult *
bmvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;
	BlockNumber preblk,
				blkno;
	Buffer		buffer,
				prevbuf;
	int			nvalues;
	BlockNumber *blocks;
	BitmapMetaPageData *meta;
	Page		page;
	BitmapPageOpaque opaque;

	if (info->analyze_only)
		return stats;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	buffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	meta = BitmapPageGetMeta(BufferGetPage(buffer));
	blkno = meta->valBlkEnd;

	nvalues = meta->ndistinct;
	if (nvalues == 0)
	{
		UnlockReleaseBuffer(buffer);
		return stats;
	}

	blocks = palloc(sizeof(BlockNumber) * nvalues);
	memcpy(blocks, meta->firstBlk, sizeof(BlockNumber) * nvalues);
	UnlockReleaseBuffer(buffer);

	for (int i = 0; i < nvalues; i++)
	{
		blkno = blocks[i];
		preblk = InvalidBlockNumber;
		while (blkno != InvalidBlockNumber)
		{
			vacuum_delay_point();

			buffer = ReadBuffer(index, blkno);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buffer);
			opaque = BitmapPageGetOpaque(page);

			if (BitmapPageDeleted(page))
			{
				RecordFreeIndexPage(index, blkno);
				stats->pages_free++;
				if (preblk != InvalidBlockNumber)
				{
					prevbuf = ReadBuffer(index, preblk);
					LockBuffer(prevbuf, BUFFER_LOCK_EXCLUSIVE);
					BitmapPageGetOpaque(BufferGetPage(prevbuf))->nextBlk = opaque->nextBlk;
					UnlockReleaseBuffer(prevbuf);
				}
				else
				{
					blocks[i] = opaque->nextBlk;
				}
			}

			preblk = blkno;
			blkno = opaque->nextBlk;
			UnlockReleaseBuffer(buffer);
		}
	}

	IndexFreeSpaceMapVacuum(info->index);

	return stats;
}
