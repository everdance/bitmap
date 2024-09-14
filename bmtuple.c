#include <postgres.h>

#include <utils/rel.h>
#include <utils/datum.h>

#include "bitmap.h"

BitmapTuple *bitmap_form_tuple(ItemPointer ctid) {
  BitmapTuple *tuple = palloc0(sizeof(BitmapTuple));
  tuple->heapblk = BlockIdGetBlockNumber(&ctid->ip_blkid);
  tuple->bm[ctid->ip_posid/32] |= 0x1 << (ctid->ip_posid%32);
  
  return tuple;
}

ItemPointer *bm_tuple_to_tids(BitmapTuple *tup, int *count) {
  int i;
  int n = 0;

  ItemPointerData *tids = palloc0(sizeof(ItemPointerData) *MAX_HEAP_TUPLE_PER_PAGE);

  for (i = 0; i < MAX_HEAP_TUPLE_PER_PAGE; i++) {
    if (0x1 << (i%32) & (tup->bm[i/32])) {
      tids[n].ip_blkid.bi_hi = tup->heapblk >> 16;
      tids[n].ip_blkid.bi_lo = tup->heapblk & 0xffff;
      tids[n].ip_posid = i+1;
      n++;
    }
  }

  *count = n;
  
  return tids;
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