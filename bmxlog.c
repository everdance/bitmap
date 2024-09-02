#include "bitmap.h"

#include <access/xlogreader.h>

static void bm_xlog_createidx(XLogReaderState *record) {}

static void bm_xlog_insert_update(XLogReaderState *record,
                                    xl_bm_insert *xlrec) {}

static void bm_xlog_insert(XLogReaderState *recor) {}

static void bm_xlog_update(XLogReaderState *record) {}

static void bm_xlog_samepage_update(XLogReaderState *record) {}

static void bm_xlog_revmap_extend(XLogReaderState *record) {}

static void bm_xlog_desummarize_page(XLogReaderState *record) {}

void bm_redo(XLogReaderState *record) {}

void bm_mask(char *pagedata, BlockNumber blkno) {}
