# Bitmap Index

This is a bitmap index access method extension for PostgreSQL. The advantageous use case of this index is for large tables with columns that have very few distinctive values. With bitmap index on these columns, the access method can generate heap tid map efficiently as the bitmap for heap tuples are organised by distinctive values and bulkly grouped in one index tuple for each heap block.

## Compile, Install and Test

To compile and install the extension:

```bash
> make
> make install
```

```bash
> make clean
```

test

```bash
> make installcheck
```

## Design

The index does not make assumption or require user's input on the number of distinctive values. However, there's a limit set by how many block number entries we can store in the single meta page. With 8192 block size, the access method can index maximumly 2042 distinctive values which is sufficient for intended bitmap use cases.

The index data is organised in three different block pages. 

### Meta Page

Meta page stores distinctive key values that stored in value pages. It also contains the last value page block number. The first block number for each distinctive values are stored as an array in the page. To find the first bitmap page for a distinctive values, we need to search the rank number of the distinctive values in the value pages first, and then use the rank as index to the block number array. Meta page block number is always zero.

```
+----------------+-------------------------------------+
| PageHeaderData | magic | ndist | valblkend |  blknum |
+-----------+----+-------------------------------------+
| ... blknum  ...								       |
+-----------+------------------------------------------+
```

### Values Page

Values page store distinctive index values that inserted. The page layout is identical to regular block storing index/heap tuples. In special space of the page, it keeps how man items stored in the page and the block number of next value page for traversing.

### Bitmap Page

Bitmap page is a regular index page. The index tuple stores a bit set indicating whether heap tuples have the distinctive values and the block number of the heap page. Each heap tuple is represented by one bit, the offset of the bit in the bit set represents the offset position of the tuple in the heap block.

## Comparison with brin/bloom index

This bitmap index differs from other bloom indexes that it does not use hashed code to represent distinctive index keys. Traditional bloom indexes requires max distinctive values option to compute the optimised hash code length. This index does not require the option, distinctive keys can simplify increase to max distinctive values internally forced by single mete page constraint. Also, it does not need to recheck due to lossy hash.

The native brin/bloom access method is a very lightweight but lossy index. It store heap block level bitmaps. It uses hash to compute the bitmap for minimumly one heap block or more commonly a range of blocks. Bloom method in contrib module indexes table at heap tuple level storing both heap tuple pointer and hashed value of index keys, it's much bulky that the bitmap index approach here. 

### Space Comparisions

Heap table

```sql
postgres=# INSERT INTO tst SELECT i%2, substr(md5(i::text), 1, 1) FROM generate_series(1,2000000) i;
INSERT 0 2000000

postgres=# select relpages from pg_class where relname = 'tst';
 relpages 
----------
     8850
(1 row)
```

Bitmap

```sql
postgres=# CREATE INDEX bitmapidx ON tst USING bitmap (i);
CREATE INDEX

postgres=# select relpages from pg_class where relname = 'bitmapidx';
 relpages 
----------
       72
(1 row)
```

Bloom

```sql
postgres=# create index bloom_idx_tst on tst using bloom (i) with (length=1, col1=1);
CREATE INDEX
postgres=# select relpages from pg_class where relname = 'bloom_idx_tst';            
 relpages 
----------
     1962
(1 row)
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
```

Btree

```sql
postgres=# create index breetst on tst using btree(i);
CREATE INDEX
postgres=# select relpages from pg_class where relname = 'breetst';
 relpages 
----------
     1695
(1 row)

```


## Page Inspection Functions

```sql
postgres=# create index bitmapidx on tst using bitmap (i);
CREATE INDEX
postgres=# select * from bm_valuep('bitmapidx',1);
 index | data 
-------+------
     1 | 1
     2 | 0
(2 rows)

postgres=# select * from bm_metap('bitmapidx');
   magic    | ndistinct | val_endblk | first_blks 
------------+-----------+------------+------------
 0xDABC9876 |         2 |          1 | 2, 3
(1 row)

postgres=# select * from bm_indexp('bitmapidx',2);
 index | heap_blk |                             bitmap                              
-------+----------+-----------------------------------------------------------------
     1 |        0 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     2 |        1 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     3 |        2 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     4 |        3 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     5 |        4 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     6 |        5 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     7 |        6 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     8 |        7 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 
     9 |        8 | AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA AAAAAAAA 0 
(9 rows)

postgres=# select * from bm_indexp('bitmapidx',3);
 index | heap_blk |                             bitmap                              
-------+----------+-----------------------------------------------------------------
     1 |        0 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     2 |        1 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     3 |        2 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     4 |        3 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     5 |        4 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     6 |        5 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     7 |        6 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     8 |        7 | 55555554 55555555 55555555 55555555 55555555 55555555 55555555 
     9 |        8 | 55555554 55555555 55555555 55555555 55555555 55555555 1 
(9 rows)

postgres=# select relpages from pg_class where relname = 'bitmapidx';
 relpages 
----------
        4
(1 row)

postgres=# select relpages from pg_class where relname = 'tst';
 relpages 
----------
        9
(1 row)

```
