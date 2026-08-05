# impala_fdw — PostgreSQL FDW for Apache Impala (Kudu-only; C/C++)
# Build with PGXS (requires pg_config on PATH).

MODULE_big = impala_fdw
OBJS = \
	src/impala_fdw.o \
	src/path_select.o \
	src/krb_util.o

EXTENSION = impala_fdw
DATA = sql/impala_fdw--0.1.0.sql

PG_CPPFLAGS += -I$(srcdir)/src

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Phase 1+: link C++ HS2 + libkudu_client
# OBJS += src/exec_impala.o src/exec_kudu.o
# SHLIB_LINK += -lkudu_client
