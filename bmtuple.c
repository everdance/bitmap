#include <postgres.h>

#include <utils/rel.h>
#include <utils/datum.h>

#include "bitmap.h"

BitmapTuple *bitmap_form_tuple(ItemPointer ctid) {
  BitmapTuple *tuple = palloc0(sizeof(BitmapTuple));
  OffsetNumber offset = ctid->ip_posid-1;
  tuple->heapblk = BlockIdGetBlockNumber(&ctid->ip_blkid);
  tuple->bm[offset/32] |= 0x1 << (offset%32);
  
  return tuple;
}

int bm_tuple_to_tids(BitmapTuple *tup, ItemPointer tids) {
  int i;
  int n = 0;

  for (i = 0; i < MAX_HEAP_TUPLE_PER_PAGE; i++) {
    if (0x1 << (i%32) & (tup->bm[i/32])) {
      tids[n].ip_blkid.bi_hi = tup->heapblk >> 16;
      tids[n].ip_blkid.bi_lo = tup->heapblk & 0xffff;
      tids[n].ip_posid = i+1;
      n++;
    }
  }

  return n;
}

int bm_tuple_next_htpid(BitmapTuple *tup, ItemPointer tid, int start) {
  int i;

  for (i = start + 1; i < MAX_HEAP_TUPLE_PER_PAGE; i++) {
    if (0x1 << (i%32) & (tup->bm[i/32])) {
      tid->ip_blkid.bi_hi = tup->heapblk >> 16;
      tid->ip_blkid.bi_lo = tup->heapblk & 0xffff;
      tid->ip_posid = i+1;
      return i;
    }
  }

  return -1;
}


bool bm_vals_equal(Relation index, Datum *cmpVals, bool *cmpIsnull, IndexTuple itup) {
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