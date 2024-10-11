-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitmap" to load this file. \quit

CREATE FUNCTION bmhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD bitmap TYPE INDEX HANDLER bmhandler;
COMMENT ON ACCESS METHOD bitmap IS 'bitmap index access method';

-- Access operator classes
CREATE OPERATOR CLASS int2_ops
DEFAULT FOR TYPE int2 USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       btint2cmp(int2,int2),
STORAGE         int2;

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       btint4cmp(int4,int4),
STORAGE         int4;

CREATE OPERATOR CLASS int8_ops
DEFAULT FOR TYPE int8 USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       btint8cmp(int8,int8),
STORAGE         int8;

CREATE OPERATOR CLASS float4_ops
DEFAULT FOR TYPE float4 USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       btfloat4cmp(float4,float4),
STORAGE         float4;

CREATE OPERATOR CLASS float8_ops
DEFAULT FOR TYPE float8 USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       btfloat8cmp(float8,float8),
STORAGE         float8;

CREATE OPERATOR CLASS timestamp_ops
DEFAULT FOR TYPE timestamp USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       timestamp_cmp(timestamp,timestamp),
STORAGE         timestamp;

CREATE OPERATOR CLASS timestamptz_ops
DEFAULT FOR TYPE timestamptz USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       timestamptz_cmp(timestamptz,timestamptz),
STORAGE         timestamptz;

CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       network_cmp(inet,inet),
STORAGE         inet;

CREATE OPERATOR CLASS cidr_ops
DEFAULT FOR TYPE cidr USING bitmap
AS
    OPERATOR        1       =(inet, inet),
    FUNCTION        1       network_cmp(inet,inet),
STORAGE         cidr;

CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       bttextcmp(text,text),
STORAGE         text;

CREATE OPERATOR CLASS varchar_ops
DEFAULT FOR TYPE varchar USING bitmap
AS
    OPERATOR        1       =(text, text),
    FUNCTION        1       bttextcmp(text,text),
STORAGE         varchar;

CREATE OPERATOR CLASS char_ops
DEFAULT FOR TYPE "char" USING bitmap
AS
    OPERATOR        1       =,
    FUNCTION        1       btcharcmp("char","char"),
STORAGE         "char";

-- Page inspection functions
CREATE FUNCTION bm_metap(IN relname text,
    OUT magic text,
    OUT ndistinct int8,
    OUT start_blks text)
AS 'MODULE_PATHNAME', 'bm_metap'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION bm_valuep(IN relname text, IN blkno int8,
    OUT index int4,
    OUT data text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bm_valuep'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION bm_indexp(IN relname text, IN blkno int8,
    OUT index int4,
    OUT heap_blk int4,
    OUT bitmap text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bm_indexp'
LANGUAGE C STRICT PARALLEL SAFE;