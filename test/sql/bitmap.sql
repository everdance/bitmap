DROP TABLE IF EXISTS test_tbl;

CREATE TABLE test_tbl (
	i	int4,
	t	text
);

CREATE INDEX bmidx ON test_tbl USING bitmap (i);

SELECT * FROM bm_metap('bmidx');

INSERT INTO test_tbl VALUES (1, 'x'), (0, 'y'), (NULL, 'N');

SELECT * FROM bm_metap('bmidx');
SELECT * FROM bm_valuep('bmidx', 1);
SELECT * FROM bm_indexp('bmidx', 2);
SELECT * FROM bm_indexp('bmidx', 3);
SELECT * FROM bm_indexp('bmidx', 4);

SET enable_seqscan=off;
EXPLAIN SELECT * FROM test_tbl WHERE i = 0;
EXPLAIN SELECT * FROM test_tbl WHERE i IS NULL;

SELECT * FROM test_tbl WHERE i = 0;
SELECT * FROM test_tbl WHERE i IS NULL;

DELETE FROM test_tbl;

INSERT INTO test_tbl SELECT i%2, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;

SET enable_seqscan=on;
SET enable_bitmapscan=off;
SET enable_indexscan=off;

SELECT count(*) FROM test_tbl WHERE i = 0;
SELECT count(*) FROM test_tbl WHERE i = 1;
SELECT count(*) FROM test_tbl WHERE i = 7;
SELECT count(*) FROM test_tbl WHERE t = '5';
SELECT count(*) FROM test_tbl WHERE i = 7 AND t = '5';

SET enable_seqscan=off;
SET enable_bitmapscan=on;
SET enable_indexscan=on;

EXPLAIN (COSTS OFF) SELECT count(*) FROM test_tbl WHERE i = 7;
EXPLAIN (COSTS OFF) SELECT count(*) FROM test_tbl WHERE t = '5';
EXPLAIN (COSTS OFF) SELECT count(*) FROM test_tbl WHERE i = 7 AND t = '5';

SELECT count(*) FROM test_tbl WHERE i = 7;
SELECT count(*) FROM test_tbl WHERE t = '5';
SELECT count(*) FROM test_tbl WHERE i = 7 AND t = '5';

DELETE FROM test_tbl;
INSERT INTO test_tbl SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;
VACUUM ANALYZE test_tbl;

SELECT count(*) FROM test_tbl WHERE i = 7;
SELECT count(*) FROM test_tbl WHERE t = '5';
SELECT count(*) FROM test_tbl WHERE i = 7 AND t = '5';

DELETE FROM test_tbl WHERE i > 1 OR t = '5';
VACUUM test_tbl;
INSERT INTO test_tbl SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;

SELECT count(*) FROM test_tbl WHERE i = 7;
SELECT count(*) FROM test_tbl WHERE t = '5';
SELECT count(*) FROM test_tbl WHERE i = 7 AND t = '5';

VACUUM FULL test_tbl;

SELECT count(*) FROM test_tbl WHERE i = 7;
SELECT count(*) FROM test_tbl WHERE t = '5';
SELECT count(*) FROM test_tbl WHERE i = 7 AND t = '5';

-- -- Try an unlogged table too

-- CREATE UNLOGGED TABLE test_tblu (
-- 	i	int4,
-- 	t	text
-- );

-- INSERT INTO test_tblu SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000) i;
-- CREATE INDEX bitmapidxu ON test_tblu USING bitmap (i, t) WITH (col2 = 4);

-- SET enable_seqscan=off;
-- SET enable_bitmapscan=on;
-- SET enable_indexscan=on;

-- EXPLAIN (COSTS OFF) SELECT count(*) FROM test_tblu WHERE i = 7;
-- EXPLAIN (COSTS OFF) SELECT count(*) FROM test_tblu WHERE t = '5';
-- EXPLAIN (COSTS OFF) SELECT count(*) FROM test_tblu WHERE i = 7 AND t = '5';

-- SELECT count(*) FROM test_tblu WHERE i = 7;
-- SELECT count(*) FROM test_tblu WHERE t = '5';
-- SELECT count(*) FROM test_tblu WHERE i = 7 AND t = '5';

-- RESET enable_seqscan;
-- RESET enable_bitmapscan;
-- RESET enable_indexscan;

-- -- Run amvalidator function on our opclasses
-- SELECT opcname, amvalidate(opc.oid)
-- FROM pg_opclass opc JOIN pg_am am ON am.oid = opcmethod
-- WHERE amname = 'bitmap'
-- ORDER BY 1;

-- --
