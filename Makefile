MODULE_big = bitmap
EXTENSION = bitmap
DATA = bitmap--1.0.sql
DOCS = README.md
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
	bitmap.o \
	bmcost.o \
	bmpage.o \
	bmscan.o \
	bmtuple.o \
	bmvacuum.o \
	bmvalidate.o \
	bmxlog.o \
	bminspect.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
