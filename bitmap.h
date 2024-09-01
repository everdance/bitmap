#ifndef _BITMAP_H_
#define _BITMAP_H_

#include <postgres.h>
#include <fmgr.h>
#include <time.h>
#include <access/amapi.h>
#include <access/itup.h>
#include <nodes/pathnodes.h>
#include <access/htup_details.h>

#define BITMAP_METAPAGE_BLKNO 0
#define BITMAP_VALPAGE_START_BLKNO 1
#define MAX_DISTINCT ((BLCKSZ - offsetof(BitmapMetaPageData, firstBlk))/sizeof(BlockNumber))

// use these funcs from indexfsm.c to manage free index page
// GetFreeIndexPage
// RecordFreeIndexPage
// RecordUsedIndexPage
// 
/// meta page ///
typedef struct BitmapMetaPageData
{
  uint32 magic;
  int ndistinct; // number of distinct values, automatically increase
  int valBlkEnd; // end value page block number
  BlockNumber firstBlk[FLEXIBLE_ARRAY_MEMBER]; // index page by distinct vals index
} BitmapMetaPageData;

#define BitmapPageGetMeta(page) ((BitmapMetaPageData *) PageGetContents(page))
// store index tuple
// depending on vals size, we might need multiple value pages for max distinct values
typedef struct BitmapValPageOpaqueData {
  int ntuples;
  BlockNumber nextBlk;
} BitmapValPageOpaqueData;

typedef BitmapValPageOpaqueData *BitmapValPageOpaque;


#define BitmapValPageGetOpaque(page)                                           \
  ((BitmapValPageOpaque)PageGetSpecialPointer(page))

// index data page accessed by PageGetSpecialPointer
typedef struct BitmapPageSpecData {
  uint16 maxoff;
  BlockNumber nextBlk;
} BitmapPageSpecData;

typedef BitmapPageSpecData *BitmapPageOpaque;

typedef struct BitmapOptions {} BitmapOptions;

#define BitmapPageGetTuple(page, off) ()

#define MAX_BITS_32 (220/32 + 1)  // defined in htup_details.h
/// index tuple ///
// heap block id
// tuple bit mpa
typedef struct BitmapTuple {
  BlockNumber heapblk;
  bits32 bm[MAX_BITS_32];
} BitmapTuple;

typedef struct BitmapState
{
  MemoryContext tmpCxt;
} BitmapState;

typedef struct BitmapBuildState
{
  int64 indtuples;
  int ndistinct;
  int valEndBlk;
  int64 count;
  MemoryContext tmpCtx;
  PGAlignedBlock *blocks[FLEXIBLE_ARRAY_MEMBER];
} BitmapBuildState;

extern bool bmvalidate(Oid opclassoid);
extern IndexScanDesc bmbeginscan(Relation r, int nkeys, int norderbys);
extern void bmrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
              ScanKey orderbys, int norderbys);
extern void bmendscan(IndexScanDesc scan);
extern int64 bmgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void bmcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
               double *indexPages);
extern IndexBulkDeleteResult *bmbulkdelete(IndexVacuumInfo *info,
                                    IndexBulkDeleteResult *stats,
                                    IndexBulkDeleteCallback callback,
                                    void *callback_state);
extern IndexBulkDeleteResult *bmvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);


#endif
