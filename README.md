# Bitmap Index

This is a bitmap index access method extension for PostgreSQL. The advantageous use case of this index is for large tables with columns that have very few distinctive values. With bitmap index on these columns, the access method can generate heap tid map efficiently as the bitmap for heap tuples are organised by distinctive values and bulkly grouped in one index tuple for each heap block.

## Install
To install the extension, download the source and enter the source directory:

```bash
> make
> make install
```

Connect to Postgres Server:
```sql
postgres=# create extension bitmap;
```

## Design

The index does not make assumption or require user's input on the number of distinctive values. However, there's a limit set by how many block number ids we can store in the single meta data block page. With 8192 block size, the access method can index maximumly 2042 distinctive values which is sufficient for intended bitmap use cases.

The index method does not use hash code to represent distinctive index keys. Traditional bloom indexes requires set max distinctive values index option to precompute optimised hash code length. In this method, distinctive key values can simplify increase automatically on index build/insert.

The native brin/bloom index method is very lightweight but lossy. It stores heap block level bitmaps. It uses hash to compute the bitmap for minimumly one heap block or more commonly a range of blocks. Bloom method in contrib module indexes stores both heap tuple pointer and hashed value of index keys, it consumes more spaces.

The index data are organised in three different block pages. 

### Meta Page

Meta page stores distinctive key values that stored in value pages. The first block page number for each distinctive key values are stored as an array in the page. To find the first bitmap page for a distinctive key values, we need to find the ordering number in value pages first, and then use the order as index to the block number array. Meta page block number is always zero.

```
+----------------+-------------------------------------+
| PageHeaderData | magic | ndist |  array of blknums   |
+-----------+----+-------------------------------------+
```

### Values Page

Values page stores distinctive index key values that are inserted. The page layout is identical to regular block storing heap tuples. In special space of the page, it keeps how many items are stored in the page and the block number of next value page.

### Bitmap Page

Bitmap page is a regular index page. A index tuple in the page stores the bitset for one heap page indicating whether each heap tuple have the distinctive values. Each heap tuple is represented by one bit, 1 means match, 0 is not. The offset of the bit in the bitset(low to high) represents the offset position of the tuple in the heap block.

## Statistics

Heap table

```sql
postgres=# INSERT INTO tst SELECT i%10, substr(md5(i::text), 1, 1) FROM generate_series(1,2000000) i;
INSERT 0 2000000

postgres=# select relpages from pg_class where relname = 'tst';
 relpages 
----------
     8850
(1 row)

postgres=# select count(*) from tst where i = 0;
 count  
--------
 200000
(1 row)

Time: 65.970 ms
```

Bitmap

```sql
postgres=# CREATE INDEX bitmapidx ON tst USING bitmap (i);
CREATE INDEX

postgres=# select relpages from pg_class where relname = 'bitmapidx';
 relpages 
----------
       402

(1 row)

postgres=# select count(*) from tst where i = 0;
 count  
--------
 200000
(1 row)

Time: 24.862 ms
```

Bloom

```sql
postgres=# create index bloom_idx_tst on tst using bloom (i) with (length=1, col1=1);
CREATE INDEX
postgres=# select relpages from pg_class where relname = 'bloom_idx_tst';            
 relpages 
----------
     3923
(1 row)

postgres=# select count(*) from tst where i = 0;
 count  
--------
 200000
(1 row)

Time: 79.667 ms
```

Brin

```sql
postgres=# create index brin_idx_tst on tst using brin (i) with (pages_per_range=1);
CREATE INDEX
postgres=# select relpages from pg_class where relname = 'brin_idx_tst';
 relpages 
----------
       30
(1 row)

postgres=# select count(*) from tst where i = 0;
 count  
--------
 200000
(1 row)

Time: 92.875 ms
```

Btree

```sql
postgres=# create index breetst on tst using btree(i);
CREATE INDEX
postgres=# select relpages from pg_class where relname = 'breetst';
 relpages 
----------
     1696
(1 row)

postgres=# select count(*) from tst where i = 0;
 count  
--------
 200000
(1 row)

Time: 31.803 ms
```


## Page Inspection Functions

```sql
postgres=# select * from bm_metap('bitmapidx');                                      
   magic    | ndistinct |           start_blks           
------------+-----------+--------------------------------
 0xDABC9876 |        10 | 6, 7, 8, 9, 10, 11, 2, 3, 4, 5
(1 row)

postgres=# select * from bm_valuep('bitmapidx',1);
 index | data 
-------+------
     1 | 1
     2 | 2
     3 | 3
     4 | 4
     5 | 5
     6 | 6
     7 | 7
     8 | 8
     9 | 9
    10 | 0
(10 rows)

postgres=# select * from bm_indexp('bitmapidx',2) limit 5;
 index | heap_blk |                                  bitmap                                  
-------+----------+--------------------------------------------------------------------------
     1 |        0 | 04010040 01004010 00401004 40100401 10040100 04010040 01004010 00000000 
     2 |        1 | 40100401 10040100 04010040 01004010 00401004 40100401 10040100 00000000 
     3 |        2 | 01004010 00401004 40100401 10040100 04010040 01004010 00401004 00000001 
     4 |        3 | 10040100 04010040 01004010 00401004 40100401 10040100 04010040 00000000 
     5 |        4 | 00401004 40100401 10040100 04010040 01004010 00401004 40100401 00000000 
(5 rows)
```
