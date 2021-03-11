# contrib/autofailover/Makefile

MODULE_big = autofailover
OBJS = autofailover_funcs.o $(WIN32RES)
EXTENSION = autofailover
DATA = autofailover--1.0.sql
PGFILEDESC = "autofailover: auto failover"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/autofailover
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
