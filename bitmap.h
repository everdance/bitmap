#ifndef _BITMAP_H_
#define _BITMAP_H_

#include <fmgr.h>
#include <time.h>
#include <access/amapi.h>
#include <access/itup.h>
#include <nodes/pathnodes.h>
#include <nodes/execnodes.h>
#include <access/htup_details.h>

#define BITMAP_MAGIC_NUMBER  0xDABC9876

#define BITMAP_NSTRATEGIES 1
#define BITMAP_METAPAGE_BLKNO 0
#define BITMAP_VALPAGE_START_BLKNO 1

#define MAX_DISTINCT ((BLCKSZ \
    -MAXALIGN(SizeOfPageHeaderData) \
    -MAXALIGN(offsetof(BitmapMetaPageData, firstBlk)) \
  ) / sizeof(BlockNumber))

typedef struct BitmapMetaPageData
{
  uint32 magic;
  uint32 ndistinct; // number of distinct values, automatically increase until max distinct
  BlockNumber valBlkEnd; // end value page block number
  BlockNumber firstBlk[FLEXIBLE_ARRAY_MEMBER]; // index page by distinct vals index
} BitmapMetaPageData;

#define BitmapPageGetMeta(page) ((BitmapMetaPageData *) PageGetContents(page))


#define BITMAP_PAGE_META 0x01
#define BITMAP_PAGE_VALUE 0x02
#define BITMAP_PAGE_INDEX 0x03

typedef struct BitmapPageSpecData {
  uint16 maxoff;
  BlockNumber nextBlk;
  uint16 pgtype;
  uint16 unused;
} BitmapPageSpecData;

typedef BitmapPageSpecData *BitmapPageOpaque;

#define BitmapPageGetOpaque(page)                                           \
  ((BitmapPageOpaque)PageGetSpecialPointer(page))


typedef struct BitmapOptions {} BitmapOptions;

#define MAX_BITS_32 (220/32 + 1)

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
  uint32 ndistinct;
  BlockNumber valEndBlk;
  BlockNumber *firstBlks;
  BlockNumber *prevBlks;
  MemoryContext tmpCtx;
  PGAlignedBlock **blocks;
} BitmapBuildState;

typedef struct xl_bm_insert
{
	BlockNumber heapBlk;
	OffsetNumber offnum;
} xl_bm_insert;


extern bytea *bmoptions(Datum reloptions, bool validate);
extern bool bminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
             Relation heapRel, IndexUniqueCheck checkUnique,
             bool indexUnchanged, IndexInfo *indexInfo);
extern IndexBuildResult *bmbuild(Relation heap, Relation index,
                           IndexInfo *indexInfo);
extern void bmbuildempty(Relation index);

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

extern bool bm_page_add_tup(Page page, BitmapTuple *tuple);
extern int bm_get_val_index(Relation index, Datum *values, bool *isnull);
extern Buffer bm_new_buffer(Relation index);
extern void bm_init_page(Page page, uint16 pgtype);
extern void bm_init_metapage(Relation index, ForkNumber fork);
extern void bm_flush_cached(Relation index, BitmapBuildState *state);


extern BitmapTuple *bitmap_form_tuple(ItemPointer ctid);
extern bool bm_vals_equal(Relation index, Datum *cmpVals, bool *cmpIsnull, IndexTuple itup);
#endif
