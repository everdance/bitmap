-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitmap" to load this file. \quit

CREATE FUNCTION bmhandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD bitmap TYPE INDEX HANDLER bmhandler;
COMMENT ON ACCESS METHOD bitmap IS 'bitmap index access method';