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

			bool		hasDelete = false;

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
				int			delsTuple = 0;

				count = bm_tuple_to_tids(itup, tids);
				for (int n = 0; n < count; n++)
				{
					if (callback(&tids[n], callback_state))
					{
						int			idx = tids[n].ip_posid - 1;

						itup->bm[idx / 32] &= ~(0x1 << (idx % 32));
						delsTuple++;
						hasDelete = true;
						stats->tuples_removed += 1;
					}
				}

				/* all tuples deleted in a single index tuple, del index tuple */
				if (delsTuple == count)
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

			if (itupPtr != itup)
			{
				if (opaque->maxoff == 0)
				{
					BitmapPageSetDeleted(page);
				}
				((PageHeader) page)->pd_lower = (Pointer) itupPtr - page;
			}
			blkno = opaque->nextBlk;

			if (hasDelete)
			{
				GenericXLogFinish(gxlogState);
			}
			else
			{
				GenericXLogAbort(gxlogState);
			}
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
	Buffer		mbuffer,
				buffer,
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

	mbuffer = ReadBuffer(index, BITMAP_METAPAGE_BLKNO);
	LockBuffer(mbuffer, BUFFER_LOCK_SHARE);
	meta = BitmapPageGetMeta(BufferGetPage(mbuffer));

	nvalues = meta->ndistinct;
	if (nvalues == 0)
	{
		UnlockReleaseBuffer(mbuffer);
		return stats;
	}

	blocks = palloc(sizeof(BlockNumber) * nvalues);
	memcpy(blocks, meta->firstBlk, sizeof(BlockNumber) * nvalues);

	for (int i = 0; i < meta->ndistinct; i++)
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
				stats->pages_free++;
				if (preblk != InvalidBlockNumber)
				{
					prevbuf = ReadBuffer(index, preblk);
					LockBuffer(prevbuf, BUFFER_LOCK_EXCLUSIVE);
					BitmapPageGetOpaque(BufferGetPage(prevbuf))->nextBlk = opaque->nextBlk;
					UnlockReleaseBuffer(prevbuf);
				}
				else
					/* first page is removed, need to update */
				{
					blocks[i] = opaque->nextBlk;
					if (blocks[i] == InvalidBlockNumber)
					{
						nvalues--;
					}
				}
				RecordFreeIndexPage(index, blkno);
			}

			preblk = blkno;
			blkno = opaque->nextBlk;
			UnlockReleaseBuffer(buffer);
		}
	}

	/* copy meta page changes to meta page */
	LockBuffer(mbuffer, BUFFER_LOCK_UNLOCK);
	LockBuffer(mbuffer, BUFFER_LOCK_EXCLUSIVE);
	memcpy(meta->firstBlk, blocks, sizeof(BlockNumber) * meta->ndistinct);
	meta->ndistinct = nvalues;
	UnlockReleaseBuffer(mbuffer);

	IndexFreeSpaceMapVacuum(info->index);

	return stats;
}
