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
  int ncols; // number of index columns
  int valBlkEnd;  // end value page block number
  BlockNumber firstBlk[MAX_DISTINCT]; // index page by distinct vals index
} BitmapMetaPageData;

// store index tuple
// depending on vals size, we might need multiple value pages for max distinct values
typedef struct BitmapValPageSpecData {
  int ntuples;
  BlockNumber nextValBlk;
} BitmapValPagSpecData; 


// index data page accessed by PageGetSpecialPointer
typedef struct BitmapPageSpecData {
  uint16 flags;
  BlockNumber heapBlkMax;
  BlockNumber heapBlkMin;
  BlockNumber nextBlk;
} BitmapPageSpecData;

/// index tuple ///
// heap block id
// tuple bit mpa
typedef struct BitmapTuple {
  BlockNumber heapblk;
  bits32 bm[MaxHeapTuplesPerPage/32]; // defined in htup_details.h
} BitmapTuple;

typedef struct BitmapState
{
} BitmapState;

#endif
