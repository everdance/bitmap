-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitmap" to load this file. \quit

CREATE FUNCTION bmhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD bitmap TYPE INDEX HANDLER bmhandler;
COMMENT ON ACCESS METHOD bitmap IS 'bitmap index access method';

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING bitmap
AS
    OPERATOR        1       =(int4, int4),
    FUNCTION        1       btint4cmp(int4,int4),
STORAGE         int4;

CREATE FUNCTION bm_metap(IN relname text,
    OUT magic text,
    OUT ndistinct int8,
    OUT val_endblk int8,
    OUT first_blks text)
AS 'MODULE_PATHNAME', 'bm_metap'
LANGUAGE C STRICT PARALLEL SAFE;