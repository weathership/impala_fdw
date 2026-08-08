# impala_fdw — PostgreSQL FDW for Impala HS2 → Kudu (C/C++ + Thrift)
#
# Build:
#   make                    # uses pg_config + thrift from THRIFT_HOME/pkg-config/nix
#   make THRIFT_HOME=...    # explicit thrift prefix
#   make IMPALA_FDW_WITH_KUDU=1 KUDU_CLIENT_LIBDIR=... KUDU_CLIENT_INCDIR=...
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

# --- libkudu_client (optional; PR-K0+) ---
# Library: .devenv/impala/lib (symlink farm) or toolchain kudu-*/{debug,release}/lib
# Headers: toolchain include — NOT under .devenv/impala (see docs/kudu_scan.md)
KUDU_VER ?= $(or $(IMPALA_KUDU_VERSION),879a8f9e2)
KUDU_CLIENT_LIBDIR ?= $(shell \
  if [ -f "$(CURDIR)/../../.devenv/impala/lib/libkudu_client.so" ]; then \
    echo "$(CURDIR)/../../.devenv/impala/lib"; \
  elif [ -n "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)" ] && \
       [ -f "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/debug/lib/libkudu_client.so" ]; then \
    echo "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/debug/lib"; \
  elif [ -n "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)" ] && \
       [ -f "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/release/lib/libkudu_client.so" ]; then \
    echo "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/release/lib"; \
  fi)
KUDU_CLIENT_INCDIR ?= $(shell \
  if [ -n "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)" ] && \
     [ -f "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/debug/include/kudu/client/client.h" ]; then \
    echo "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/debug/include"; \
  elif [ -n "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)" ] && \
     [ -f "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/release/include/kudu/client/client.h" ]; then \
    echo "$(IMPALA_TOOLCHAIN_PACKAGES_HOME)/kudu-$(KUDU_VER)/release/include"; \
  elif [ -f "$(CURDIR)/../kudu/src/kudu/client/client.h" ]; then \
    echo "$(CURDIR)/../kudu/src"; \
  fi)

# Auto-enable on Linux when both lib and headers resolve (devenv may also set =1)
ifeq ($(IMPALA_FDW_WITH_KUDU),)
  ifneq ($(KUDU_CLIENT_LIBDIR),)
    ifneq ($(KUDU_CLIENT_INCDIR),)
      IMPALA_FDW_WITH_KUDU := 1
    endif
  endif
endif

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
	src/deparse.o \
	src/path_select.o \
	src/krb_util.o \
	src/exec_impala.o \
	gen-cpp/TCLIService.o \
	gen-cpp/TCLIService_types.o \
	gen-cpp/TCLIService_constants.o

ifeq ($(IMPALA_FDW_WITH_KUDU),1)
  ifeq ($(KUDU_CLIENT_LIBDIR),)
    $(error IMPALA_FDW_WITH_KUDU=1 but KUDU_CLIENT_LIBDIR unset — need libkudu_client.so)
  endif
  ifeq ($(KUDU_CLIENT_INCDIR),)
    $(error IMPALA_FDW_WITH_KUDU=1 but KUDU_CLIENT_INCDIR unset — need kudu/client/client.h)
  endif
  PG_CPPFLAGS += -I$(KUDU_CLIENT_INCDIR) -DIMPALA_FDW_WITH_KUDU=1
  # PR-K5b: libkrb5 for optional keytab→ccache kinit before KuduClientBuilder
  ifneq ($(SIG_KRB5_INC),)
    PG_CPPFLAGS += -I$(SIG_KRB5_INC)
  endif
  SHLIB_LINK += -L$(KUDU_CLIENT_LIBDIR) -lkudu_client \
    -Wl,-rpath,$(KUDU_CLIENT_LIBDIR)
  ifneq ($(SIG_KRB5_LIB),)
    SHLIB_LINK += -L$(SIG_KRB5_LIB) -Wl,-rpath,$(SIG_KRB5_LIB)
  endif
  SHLIB_LINK += $(shell pkg-config --libs krb5 2>/dev/null || echo -lkrb5)
  OBJS += src/exec_kudu.o src/kudu_pred.o
endif

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

src/exec_kudu.o: src/exec_kudu.cpp src/exec_kudu.h
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(PG_CPPFLAGS) -std=c++17 -fPIC -c -o $@ src/exec_kudu.cpp

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
