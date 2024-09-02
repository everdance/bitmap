#include "bitmap.h"

IndexScanDesc bmbeginscan(Relation r, int nkeys, int norderbys) {}

void bmrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
              ScanKey orderbys, int norderbys) {}

void bmendscan(IndexScanDesc scan) {}

int64 vbmgetbitmap(IndexScanDesc scan, TIDBitmap *tbm){}
