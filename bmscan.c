#include <postgres.h>

#include <access/relscan.h>
#include <storage/bufmgr.h>

#include "bitmap.h"

static void init_scan_opaque(BitmapScanOpaque so) {
    so->keyIndex = -1;
    so->firstBlk = InvalidBlockNumber;
    so->currentBlk = InvalidBlockNumber;
    so->offset = 0;
    so->tupleOffset = 0;
}

IndexScanDesc bmbeginscan(Relation r, int nkeys, int norderbys) {
    IndexScanDesc scan;
    BitmapScanOpaque so;

    scan = RelationGetIndexScan(r, nkeys, norderbys);

    so = (BitmapScanOpaque) palloc0(sizeof(BitmapScanOpaqueData));
    init_scan_opaque(so);
    scan->opaque = so;

    return scan;
}

void bmrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
              ScanKey orderbys, int norderbys) {
    BitmapScanOpaque so = (BitmapScanOpaque) scan->opaque;
    init_scan_opaque(so);

    if (scankey && scan->numberOfKeys > 0) {
        memmove(scan->keyData, scankey, 
        scan->numberOfKeys * sizeof(ScanKeyData));
    }
}

void bmendscan(IndexScanDesc scan) {

}

bool
bmgettuple(IndexScanDesc scan, ScanDirection dir) 
{
    scan->xs_recheck = false;
}

int64 bmgetbitmap(IndexScanDesc scan, TIDBitmap *tbm){
    int64 ntids = 0;
    BitmapScanOpaque so = (BitmapScanOpaque) scan->opaque;
    Relation index = scan->indexRelation;
    int i;
    BlockNumber blkno;
    Datum values[INDEX_MAX_KEYS];
    bool  isnull[INDEX_MAX_KEYS];
    Page page;
    Buffer buffer;
    BitmapPageOpaque opaque;
    OffsetNumber offset, maxoffset;
    BitmapTuple *itup;
    ItemPointer  *tids;

    if (so->keyIndex < 0) {
        ScanKey skey = scan->keyData;

        for (i = 0; i < scan->numberOfKeys; i++) {
            if (skey->sk_flags & SK_ISNULL) {
                isnull[i] = true;
                continue;
            }
            values[i] = skey->sk_argument;
            skey++;
        }

        so->keyIndex = bm_get_val_index(index, values, isnull);
        // keys are not indexed
        if (so->keyIndex < 0)
            return 0;
        
        so->firstBlk = bm_get_firstblk(index, so->keyIndex);
        so->currentBlk = so->firstBlk;
    }

    blkno = so->currentBlk;
    while (blkno != InvalidBlockNumber) {
        buffer = ReadBuffer(index, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buffer);
        opaque = BitmapPageGetOpaque(page);
        maxoffset = opaque->maxoff;

        for (offset = 1; offset <= maxoffset; offset++) {
            int count = 0;
            itup = BitmapPageGetTuple(page, offset);
            tids = bm_tuple_to_tids(itup, &count);
            tbm_add_tuples(tbm, tids, count, false);
            ntids += count;
        }
    }

    return ntids;
}
