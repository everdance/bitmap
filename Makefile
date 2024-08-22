MODULE_big = bitmap
EXTENSION = bitmap
DATA = bitmap--1.0.sql
PGFILEDESC = "bitmap access method"
REGRESS := bitmap
PG_USER = postgres
REGRESS_OPTS := \
	--load-extension=$(EXTENSION) \
	--user=$(PG_USER) \
	--inputdir=test \
	--outputdir=test \
	--temp-instance=${PWD}/tmpdb

OBJS = \
	bitmap.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
