CREATE TABLE tst (
	i	int4,
	t	text
);

INSERT INTO tst SELECT i%2, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;
CREATE INDEX bitmapidx ON tst USING bitmap (i);

SET enable_seqscan=on;
SET enable_bitmapscan=off;
SET enable_indexscan=off;

SELECT count(*) FROM tst WHERE i = 0;
SELECT count(*) FROM tst WHERE i = 1;

-- SET enable_seqscan=off;
-- SET enable_bitmapscan=on;
-- SET enable_indexscan=on;

-- EXPLAIN (COSTS OFF) SELECT count(*) FROM tst WHERE i = 7;
-- EXPLAIN (COSTS OFF) SELECT count(*) FROM tst WHERE t = '5';
-- EXPLAIN (COSTS OFF) SELECT count(*) FROM tst WHERE i = 7 AND t = '5';

-- SELECT count(*) FROM tst WHERE i = 7;
-- SELECT count(*) FROM tst WHERE t = '5';
-- SELECT count(*) FROM tst WHERE i = 7 AND t = '5';

-- DELETE FROM tst;
-- INSERT INTO tst SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;
-- VACUUM ANALYZE tst;

-- SELECT count(*) FROM tst WHERE i = 7;
-- SELECT count(*) FROM tst WHERE t = '5';
-- SELECT count(*) FROM tst WHERE i = 7 AND t = '5';

-- DELETE FROM tst WHERE i > 1 OR t = '5';
-- VACUUM tst;
-- INSERT INTO tst SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;

-- SELECT count(*) FROM tst WHERE i = 7;
-- SELECT count(*) FROM tst WHERE t = '5';
-- SELECT count(*) FROM tst WHERE i = 7 AND t = '5';

-- VACUUM FULL tst;

-- SELECT count(*) FROM tst WHERE i = 7;
-- SELECT count(*) FROM tst WHERE t = '5';
-- SELECT count(*) FROM tst WHERE i = 7 AND t = '5';

-- -- Try an unlogged table too

-- CREATE UNLOGGED TABLE tstu (
-- 	i	int4,
-- 	t	text
-- );

-- INSERT INTO tstu SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;
-- CREATE INDEX bitmapidxu ON tstu USING bitmap (i, t) WITH (col2 = 4);

-- SET enable_seqscan=off;
-- SET enable_bitmapscan=on;
-- SET enable_indexscan=on;

-- EXPLAIN (COSTS OFF) SELECT count(*) FROM tstu WHERE i = 7;
-- EXPLAIN (COSTS OFF) SELECT count(*) FROM tstu WHERE t = '5';
-- EXPLAIN (COSTS OFF) SELECT count(*) FROM tstu WHERE i = 7 AND t = '5';

-- SELECT count(*) FROM tstu WHERE i = 7;
-- SELECT count(*) FROM tstu WHERE t = '5';
-- SELECT count(*) FROM tstu WHERE i = 7 AND t = '5';

-- RESET enable_seqscan;
-- RESET enable_bitmapscan;
-- RESET enable_indexscan;

-- -- Run amvalidator function on our opclasses
-- SELECT opcname, amvalidate(opc.oid)
-- FROM pg_opclass opc JOIN pg_am am ON am.oid = opcmethod
-- WHERE amname = 'bitmap'
-- ORDER BY 1;

-- --
-- -- relation options
-- --
-- DROP INDEX bitmapidx;
-- CREATE INDEX bitmapidx ON tst USING bitmap (i, t) WITH (length=7, col1=4);
-- SELECT reloptions FROM pg_class WHERE oid = 'bitmapidx'::regclass;
-- -- check for min and max values
-- \set VERBOSITY terse
-- CREATE INDEX bitmapidx2 ON tst USING bitmap (i, t) WITH (length=0);
-- CREATE INDEX bitmapidx2 ON tst USING bitmap (i, t) WITH (col1=0);
