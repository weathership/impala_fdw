# impala_fdw — PostgreSQL FDW for Impala HS2 → Kudu (C/C++ + Thrift)
#
# Build:
#   make                    # uses pg_config + thrift from THRIFT_HOME/pkg-config/nix
#   make THRIFT_HOME=...    # explicit thrift prefix
#
# Generate thrift stubs (optional; gen-cpp may be committed):
#   make thrift-gen

MODULE_big = impala_fdw

# --- Thrift + Boost (prefer devenv: thrift, boost.dev on PATH/NIX) ---
THRIFT_HOME ?= $(shell pkg-config --variable=prefix thrift 2>/dev/null)
ifeq ($(THRIFT_HOME),)
  THRIFT_BIN := $(shell command -v thrift 2>/dev/null)
  ifneq ($(THRIFT_BIN),)
    THRIFT_HOME := $(shell dirname $$(dirname $(THRIFT_BIN)))
  endif
endif
BOOST_HOME ?= $(BOOST_ROOT)

PG_CPPFLAGS += -I$(srcdir)/src -I$(srcdir)/gen-cpp
ifneq ($(THRIFT_HOME),)
  PG_CPPFLAGS += -I$(THRIFT_HOME)/include
  SHLIB_LINK += -L$(THRIFT_HOME)/lib -Wl,-rpath,$(THRIFT_HOME)/lib
endif
ifneq ($(BOOST_HOME),)
  PG_CPPFLAGS += -I$(BOOST_HOME)/include
endif
SHLIB_LINK += -lthrift -lstdc++

# Skip LLVM bitcode (C++ thrift objects break clang -emit-llvm here)
with_llvm = no
OBJS = \
	src/impala_fdw.o \
	src/path_select.o \
	src/krb_util.o \
	src/exec_impala.o \
	gen-cpp/TCLIService.o \
	gen-cpp/TCLIService_types.o \
	gen-cpp/TCLIService_constants.o

EXTENSION = impala_fdw
DATA = sql/impala_fdw--0.1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# C++ compile flags for thrift objects
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -std=c++17 -fPIC -c -o $@ $<

src/exec_impala.o: src/exec_impala.cpp src/exec_impala.h
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -std=c++17 -fPIC -c -o $@ src/exec_impala.cpp

gen-cpp/%.o: gen-cpp/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -std=c++17 -fPIC -c -o $@ $<

.PHONY: thrift-gen
thrift-gen:
	@test -n "$(THRIFT)" || THRIFT=$$(command -v thrift); \
	test -x "$$THRIFT" || THRIFT=$(THRIFT_HOME)/bin/thrift; \
	"$$THRIFT" -r --gen cpp -o $(srcdir) $(srcdir)/thrift/TCLIService.thrift

# Optional smoke client (not installed)
.PHONY: hs2-smoke
hs2-smoke: tools/hs2_smoke
tools/hs2_smoke: tools/hs2_smoke.cpp src/exec_impala.o gen-cpp/TCLIService.o gen-cpp/TCLIService_types.o gen-cpp/TCLIService_constants.o
	mkdir -p tools
	$(CXX) -std=c++17 -I$(srcdir)/src -I$(srcdir)/gen-cpp -I$(THRIFT_HOME)/include \
		-o $@ tools/hs2_smoke.cpp src/exec_impala.o \
		gen-cpp/TCLIService.o gen-cpp/TCLIService_types.o gen-cpp/TCLIService_constants.o \
		-L$(THRIFT_HOME)/lib -lthrift -Wl,-rpath,$(THRIFT_HOME)/lib
