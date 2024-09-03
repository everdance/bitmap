#include <postgres.h>

#include "bitmap.h"

IndexScanDesc bmbeginscan(Relation r, int nkeys, int norderbys) {
    return NULL;
}

void bmrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
              ScanKey orderbys, int norderbys) {}

void bmendscan(IndexScanDesc scan) {}

int64 bmgetbitmap(IndexScanDesc scan, TIDBitmap *tbm){
    return 0;
}
