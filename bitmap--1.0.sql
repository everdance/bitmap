-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bitmap" to load this file. \quit

CREATE FUNCTION bitmaphandler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD bitmap TYPE INDEX HANDLER bitmaphandler;
COMMENT ON ACCESS METHOD bitmap IS 'bitmap index access method';

-- Opclasses

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING bitmap AS
	OPERATOR	1	=(int4, int4),
	FUNCTION	1	hashint4(int4);

CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING bitmap AS
	OPERATOR	1	=(text, text),
	FUNCTION	1	hashtext(text);
