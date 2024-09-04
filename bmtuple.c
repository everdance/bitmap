#include <postgres.h>

#include <utils/rel.h>
#include <utils/datum.h>

#include "bitmap.h"

BitmapTuple *bitmap_form_tuple(ItemPointer ctid) {
  BitmapTuple *tuple = palloc0(sizeof(BitmapTuple));
  tuple->heapblk = BlockIdGetBlockNumber(&ctid->ip_blkid);
  tuple->bm[ctid->ip_posid/32] &= 0x1 << (ctid->ip_posid%32);
  
  return tuple;
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