#include <postgres.h>

#include <storage/bufmgr.h>
#include <storage/indexfsm.h>

#include "bitmap.h"

bool bm_page_add_item(Page page, BitmapTuple *tuple) {
  BitmapTuple *itup;
  BitmapPageOpaque opaque;
  Pointer ptr;
  ItemId itid;

  opaque = BitmapPageGetOpaque(page);
  for (int i = 1; i <= opaque->maxoff; i++) { 
    itid = PageGetItemId(page, i);
    itup = (BitmapTuple *)PageGetItem(page, itid);
    if (itup->heapblk == tuple->heapblk) {
      for (int j = 0; j < MAX_BITS_32; j++) {
        itup->bm[j] &= tuple->bm[j];
      }
      return true;
    }
  }

  if (PageGetFreeSpace(page) < sizeof(BitmapTuple))
    return false;

  itid = PageGetItemId(page, opaque->maxoff + 1);
  itup = (BitmapTuple *)PageGetItem(page, itid);
  memcpy((Pointer) itup, (Pointer) tuple, sizeof(BitmapTuple));

	/* Adjust maxoff and pd_lower */
  opaque->maxoff++;
  itid = PageGetItemId(page, opaque->maxoff + 1);
	ptr = (Pointer) PageGetItem(page, itid);
	((PageHeader) page)->pd_lower = ptr - page;

  return true;
}

int bm_get_val_index(Relation index, Datum *values, bool *isnull) {
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
      if (bm_vals_equal(index, values, isnull, idxtuple)) {
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

Buffer bm_new_buffer(Relation index) {
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

void bm_init_page(Page page) {}

void bm_fill_metapage(Relation index, Page meta) {}

void bm_init_metapage(Relation index) {}

void bm_flush_cached(Relation index, BitmapBuildState *state) {}
