#include <postgres.h>

#include <access/relscan.h>
#include <storage/bufmgr.h>

#include "bitmap.h"

static void
reset_scan_for_next_page(BitmapScanOpaque so)
{
	so->offset = 0;
	so->maxoffset = 0;
	so->htupidx = 0;
}

static void
init_scan_opaque(BitmapScanOpaque so)
{
	so->keyIndex = -1;
	so->curPage = NULL;
	so->curBlk = InvalidBlockNumber;
	reset_scan_for_next_page(so);
}

IndexScanDesc
bmbeginscan(Relation r, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	BitmapScanOpaque so;

	scan = RelationGetIndexScan(r, nkeys, norderbys);

	so = (BitmapScanOpaque) palloc0(sizeof(BitmapScanOpaqueData));
	init_scan_opaque(so);
	scan->opaque = so;

	return scan;
}

void
bmrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{
	BitmapScanOpaque so = (BitmapScanOpaque) scan->opaque;

	init_scan_opaque(so);

	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}
}

void
bmendscan(IndexScanDesc scan)
{
	BitmapScanOpaque so = (BitmapScanOpaque) scan->opaque;

	if (so->curPage)
		pfree(so->curPage);
}

bool
bmgettuple(IndexScanDesc scan, ScanDirection dir)
{
	BitmapScanOpaque so = (BitmapScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	Buffer		buffer;
	BitmapMetaPageData *meta;
	BitmapPageOpaque opaque;
	int32		htupidx;
	BitmapTuple *itup;
	int			i;
	ItemPointerData ipd;

	scan->xs_recheck = false;

	if (so->keyIndex < 0)
	{
		ScanKey		skey = scan->keyData;

		memset(isnull, 0, sizeof(bool) * INDEX_MAX_KEYS);

		for (i = 0; i < scan->numberOfKeys; i++)
		{
			isnull[i] = false;
			if (skey->sk_flags & SK_ISNULL)
			{
				isnull[i] = true;
				continue;
			}
			values[i] = skey->sk_argument;
			skey++;
		}

		meta = bm_get_meta(index);
		if (meta->ndistinct > 0)
		{
			so->keyIndex = bm_get_val_index(index, values, isnull);
		}

		if (so->keyIndex < 0)
			return false;

		so->curBlk = meta->startBlk[so->keyIndex];
		if (so->curPage == NULL)
		{
			so->curPage = (Page) palloc(sizeof(PGAlignedBlock));
		}
	}


	while (so->curBlk != InvalidBlockNumber)
	{
		if (so->offset == 0)
		{
			buffer = ReadBuffer(index, so->curBlk);
			LockBuffer(buffer, BUFFER_LOCK_SHARE);
			memcpy(so->curPage, BufferGetPage(buffer), sizeof(PGAlignedBlock));
			UnlockReleaseBuffer(buffer);

			opaque = BitmapPageGetOpaque(so->curPage);
			so->maxoffset = opaque->maxoff;
			so->offset = 1;
		}

		itup = BitmapPageGetTuple(so->curPage, so->offset);
		htupidx = bm_tuple_next_htpid(itup, &ipd, so->htupidx + 1);

		if (htupidx >= 0)
		{
			so->htupidx = htupidx;
			scan->xs_heaptid = ipd;
			scan->xs_heap_continue = true;
			return true;
		}

		so->htupidx = 0;
		if (so->offset < so->maxoffset)
		{
			so->offset++;
			continue;
		}

		opaque = BitmapPageGetOpaque(so->curPage);
		so->curBlk = opaque->nextBlk;
		reset_scan_for_next_page(so);
	}

	scan->xs_heap_continue = false;
	return false;
}

int64
bmgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	int64		ntids = 0;
	BitmapScanOpaque so = (BitmapScanOpaque) scan->opaque;
	BitmapMetaPageData *meta;
	Relation	index = scan->indexRelation;
	int			i;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	Page		page;
	Buffer		buffer;
	BitmapPageOpaque opaque;
	OffsetNumber offset,
				maxoffset;
	BitmapTuple *itup;
	ItemPointer tids = palloc0(sizeof(ItemPointerData) * MAX_HEAP_TUPLE_PER_PAGE);

	if (so->keyIndex < 0)
	{
		ScanKey		skey = scan->keyData;

		memset(isnull, 0, sizeof(bool) * INDEX_MAX_KEYS);

		for (i = 0; i < scan->numberOfKeys; i++)
		{
			if (skey->sk_flags & SK_ISNULL)
			{
				isnull[i] = true;
				continue;
			}
			values[i] = skey->sk_argument;
			skey++;
		}

		meta = bm_get_meta(index);
		if (meta->ndistinct > 0)
		{
			so->keyIndex = bm_get_val_index(index, values, isnull);
		}

		/* keys are not indexed */
		if (so->keyIndex < 0)
			return 0;

		so->curBlk = meta->startBlk[so->keyIndex];
	}

	while (so->curBlk != InvalidBlockNumber)
	{
		buffer = ReadBuffer(index, so->curBlk);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		opaque = BitmapPageGetOpaque(page);
		maxoffset = opaque->maxoff;

		for (offset = 1; offset <= maxoffset; offset++)
		{
			int			count = 0;

			itup = BitmapPageGetTuple(page, offset);
			count = bm_tuple_to_tids(itup, tids);
			tbm_add_tuples(tbm, tids, count, false);
			ntids += count;
		}

		so->curBlk = opaque->nextBlk;
		UnlockReleaseBuffer(buffer);
	}

	return ntids;
}
