#include <postgres.h>

#include <funcapi.h>
#include <storage/bufmgr.h>
#include <access/relation.h>
#include <catalog/namespace.h>
#include <utils/varlena.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <miscadmin.h>

#include "bitmap.h"

static void values_to_string(StringInfo s, TupleDesc tupdesc, Datum *values, bool *nulls);

static Relation _bm_get_relation_by_name(text *name) {
    Relation	rel;
	RangeVar   *relrv;

    if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(name));
	rel = relation_openrv(relrv, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
        	 errmsg("\"%s\" is not a %s index",
						RelationGetRelationName(rel), "bitmap")));
    return rel;
}

PG_FUNCTION_INFO_V1(bm_metap);

/* -------------------------------------
 * Get bitmap meta page information
 * 
 * Usage: SELECT * FROM bm_metap('index_name')
 */

Datum bm_metap(PG_FUNCTION_ARGS) {
    text	   *relname = PG_GETARG_TEXT_PP(0);
    Relation rel = _bm_get_relation_by_name(relname);
	Datum		result;
	BitmapMetaPageData *meta;
	Buffer		buffer;
	Page		page;
	HeapTuple	tuple;
    TupleDesc	tupleDesc;
    int         max_block_shown = 10;
    int         i;
	int			j;
	char	   *values[4];
    StringInfoData strinfo;

    buffer = ReadBuffer(rel, 0);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);
	meta = BitmapPageGetMeta(page);

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

    j = 0;
    values[j++] = psprintf("0x%X", meta->magic);
    values[j++] = psprintf("%u", meta->ndistinct);
    values[j++] = psprintf("%u", meta->valBlkEnd);

    initStringInfo(&strinfo);
    for (i = 0; i < meta->ndistinct && i < max_block_shown; i++) {
        if (i > 0) appendStringInfoString(&strinfo, ", ");
        appendStringInfoString(&strinfo, psprintf("%u", meta->firstBlk[i]));
    }
    values[j++] = strinfo.data;

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc), values);
	result = HeapTupleGetDatum(tuple);

	UnlockReleaseBuffer(buffer);
	relation_close(rel, AccessShareLock);

	PG_RETURN_DATUM(result);
}


/*
 * cross-call data structure for SRF
 */
struct CrossCallData
{
	Page		page;
	OffsetNumber offset;
    TupleDesc  indexTupDesc;
	TupleDesc	tupd;
};


PG_FUNCTION_INFO_V1(bm_valuep);

/* -------------------------------------
 * Get bitmap value page information
 * 
 * Usage: SELECT * FROM bm_valuep('index_name', blkno)
 */

Datum bm_valuep(PG_FUNCTION_ARGS) {
    text	   *relname = PG_GETARG_TEXT_PP(0);
    BlockNumber blkno = PG_GETARG_INT32(1);
    FuncCallContext *fctx;
	MemoryContext mctx;
    struct CrossCallData *ccdata;

    if (SRF_IS_FIRSTCALL())
	{
        Relation rel;
        Buffer buffer;
        TupleDesc	tupleDesc;
        fctx = SRF_FIRSTCALL_INIT();
        rel = _bm_get_relation_by_name(relname);

        if (blkno < BITMAP_VALPAGE_START_BLKNO || blkno > MaxBlockNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid block number")));
        
        buffer = ReadBuffer(rel, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

        mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		ccdata = palloc(sizeof(struct CrossCallData));
		ccdata->page = palloc(BLCKSZ);
		memcpy(ccdata->page, BufferGetPage(buffer), BLCKSZ);
        ccdata->indexTupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
        fctx->max_calls = PageGetMaxOffsetNumber(ccdata->page);

		UnlockReleaseBuffer(buffer);
		relation_close(rel, AccessShareLock);

        if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		tupleDesc = BlessTupleDesc(tupleDesc);
        ccdata->tupd = CreateTupleDescCopy(tupleDesc);
        ccdata->offset = FirstOffsetNumber;
        fctx->user_fctx = ccdata;
        
        MemoryContextSwitchTo(mctx);
    }

    fctx = SRF_PERCALL_SETUP();
	ccdata = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
        ItemId		itid = PageGetItemId(ccdata->page, ccdata->offset);
        IndexTuple	idxtuple = (IndexTuple) PageGetItem(ccdata->page, itid);
        Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
        Datum	    rvalues[2];
        bool        rnull[2] = {false, true};
        StringInfoData s;
        HeapTuple tuple;

        rvalues[0] = UInt16GetDatum(ccdata->offset);
        ccdata->offset++;

        index_deform_tuple(idxtuple, ccdata->indexTupDesc, values, isnull);

        for (int i = 0; i < ccdata->indexTupDesc->natts; i++)
            if (!isnull[i]) rnull[1] = false;

        initStringInfo(&s);
        values_to_string(&s, ccdata->indexTupDesc, values, isnull);

        rvalues[1] = PointerGetDatum(cstring_to_text(s.data));
		tuple = heap_form_tuple(ccdata->tupd, rvalues, rnull);

		SRF_RETURN_NEXT(fctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(fctx);
}

PG_FUNCTION_INFO_V1(bm_indexp);

/* -------------------------------------
 * Get bitmap value page information
 * 
 * Usage: SELECT * FROM bm_indexp('index_name', blkno)
 */

Datum bm_indexp(PG_FUNCTION_ARGS) {
    text	   *relname = PG_GETARG_TEXT_PP(0);
    BlockNumber blkno = PG_GETARG_INT32(1);
    FuncCallContext *fctx;
	MemoryContext mctx;
    struct CrossCallData *ccdata;

    if (SRF_IS_FIRSTCALL())
	{
        Relation rel;
        Buffer buffer;
        TupleDesc	tupleDesc;

        fctx = SRF_FIRSTCALL_INIT();
        rel = _bm_get_relation_by_name(relname);

        if (blkno < BITMAP_VALPAGE_START_BLKNO + 1 || blkno > MaxBlockNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid block number")));
        
        buffer = ReadBuffer(rel, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

        mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		ccdata = palloc(sizeof(struct CrossCallData));
		ccdata->page = palloc(BLCKSZ);
		memcpy(ccdata->page, BufferGetPage(buffer), BLCKSZ);
        fctx->max_calls = BitmapPageGetOpaque(ccdata->page)->maxoff;

		UnlockReleaseBuffer(buffer);
		relation_close(rel, AccessShareLock);

        if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		tupleDesc = BlessTupleDesc(tupleDesc);
        ccdata->tupd = CreateTupleDescCopy(tupleDesc);
        ccdata->offset = FirstOffsetNumber;
        fctx->user_fctx = ccdata;
        
        MemoryContextSwitchTo(mctx);
    }

    fctx = SRF_PERCALL_SETUP();
	ccdata = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
        BitmapTuple*	bmtuple = BitmapPageGetTuple(ccdata->page, ccdata->offset);
        Datum	    rvalues[3];
        bool        rnull[3] = {false, false, false};
        StringInfoData s;
        HeapTuple tuple;
        int        i;

        rvalues[0] = UInt16GetDatum(ccdata->offset);
        rvalues[1] = UInt32GetDatum(bmtuple->heapblk);
        ccdata->offset++;

        initStringInfo(&s);
        for (i = 0; i < MAX_BITS_32; i++)
            appendStringInfo(&s,"%X ", bmtuple->bm[i]);

        rvalues[2] = PointerGetDatum(cstring_to_text(s.data));
		tuple = heap_form_tuple(ccdata->tupd, rvalues, rnull);

		SRF_RETURN_NEXT(fctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(fctx);
}

static void values_to_string(StringInfo s, TupleDesc tupdesc, Datum *values, bool *nulls) {
    int natt;

    for (natt = 0; natt < tupdesc->natts; natt++) {
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		bool		isnull;		/* column is null? */

        attr = TupleDescAttr(tupdesc, natt);
        typid = attr->atttypid;
        origval = values[natt];
        isnull = nulls[natt];

        if (natt > 0) appendStringInfoChar(s, ',');

        if (isnull) {
            appendStringInfoString(s, "null");
            continue;
        }

        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        if (!typisvarlena)
        {
            char *val = OidOutputFunctionCall(typoutput, origval);
            appendStringInfoString(s, val);
        }
        else {
            Datum val;
            val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            appendStringInfoString(s, OidOutputFunctionCall(typoutput, val));
        }
    }
}
