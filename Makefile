# impala_fdw — PostgreSQL FDW for Apache Impala
# Build with PGXS (requires pg_config on PATH).

MODULE_big = impala_fdw
OBJS = src/impala_fdw.o

EXTENSION = impala_fdw
DATA = sql/impala_fdw--0.1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Optional: extra CFLAGS for HS2 client later
# PG_CPPFLAGS += -I$(HS2_INCLUDE)
