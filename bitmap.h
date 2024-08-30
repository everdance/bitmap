#ifndef _BITMAP_H_
#define _BITMAP_H_

#include <postgres.h>
#include <fmgr.h>
#include <access/amapi.h>
#include <access/itup.h>
#include <nodes/pathnodes.h>
#include <time.h>

#define BITMAP_METAPAGE_BLKNO 0
#define BITMAP_VALPAGE_START_BLKNO 1
#define MAX_DISTINCT ((BLCKSZ - offsetof(BitmapMetaPageData, firstBlk))/sizeof(BlockNumer))

// do not support varlena type

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
  BlockNumber firstBlk[MAX_DISTINCT]; // index page by distinct vals index
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

typedef BitmapPageSpecData BitmapValPageOpaque;

#define BitmapPageGetTuple(page, off) ()

#define MAX_BITS_32 (MaxHeapTuplesPerPage/32 + 1)  // defined in htup_details.h
/// index tuple ///
// heap block id
// tuple bit mpa
typedef struct BitmapTuple {
  BlockNumber heapblk;
  bits32 bm[MAX_BITS_32];
} BitmapTuple;

typedef struct BitmapState
{
} BitmapState;

typedef struct
{
  int64 indtuples;
  MemoryContext tmpCtx;
  PGAlignedBlock *blocks[MAX_DISTINCT];
} BitmapBuildState;

#endif
