CLEAN_FILES = # deliberately empty, so we can append below.
CXX=g++
PLATFORM_LDFLAGS= -lpthread -lrt
PLATFORM_CXXFLAGS= -std=c++11 -fno-builtin-memcmp -msse -msse4.2 
PROFILING_FLAGS=-pg
OPT=
LDFLAGS += -Wl,-rpath=$(RPATH)

# DEBUG_LEVEL can have two values:
# * DEBUG_LEVEL=2; this is the ultimate debug mode. It will compile pika_hub
# without any optimizations. To compile with level 2, issue `make dbg`
# * DEBUG_LEVEL=0; this is the debug level we use for release. If you're
# running pika_hub in production you most definitely want to compile pika_hub
# with debug level 0. To compile with level 0, run `make`,

# Set the default DEBUG_LEVEL to 0
DEBUG_LEVEL?=0

ifeq ($(MAKECMDGOALS),dbg)
  DEBUG_LEVEL=2
endif

# compile with -O2 if debug level is not 2
ifneq ($(DEBUG_LEVEL), 2)
OPT += -O2 -fno-omit-frame-pointer
# if we're compiling for release, compile without debug code (-DNDEBUG) and
# don't treat warnings as errors
OPT += -DNDEBUG
DISABLE_WARNING_AS_ERROR=1
# Skip for archs that don't support -momit-leaf-frame-pointer
ifeq (,$(shell $(CXX) -fsyntax-only -momit-leaf-frame-pointer -xc /dev/null 2>&1))
OPT += -momit-leaf-frame-pointer
endif
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += $(PROFILING_FLAGS)
DEBUG_SUFFIX = "_debug"
endif

# Link tcmalloc if exist
dummy := $(shell ("$(CURDIR)/detect_environment" "$(CURDIR)/make_config.mk"))
include make_config.mk
CLEAN_FILES += $(CURDIR)/make_config.mk
PLATFORM_LDFLAGS += $(TCMALLOC_LDFLAGS)

# ----------------------------------------------
OUTPUT = $(CURDIR)/output
THIRD_PATH = $(CURDIR)/third
SRC_PATH = $(CURDIR)/src

# ----------------Dependences-------------------

ifndef SLASH_PATH
SLASH_PATH = $(THIRD_PATH)/slash
endif
SLASH = $(SLASH_PATH)/slash/lib/libslash$(DEBUG_SUFFIX).a

ifndef PINK_PATH
PINK_PATH = $(THIRD_PATH)/pink
endif
PINK = $(PINK_PATH)/pink/lib/libpink$(DEBUG_SUFFIX).a

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(THIRD_PATH)/rocksdb
endif
ROCKSDB = $(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a

ifndef FLOYD_PATH
FLOYD_PATH = $(THIRD_PATH)/floyd
endif
FLOYD = $(FLOYD_PATH)/floyd/lib/libfloyd$(DEBUG_SUFFIX).a

ifndef ROCKSUTIL_PATH
ROCKSUTIL_PATH = $(THIRD_PATH)/rocksutil
endif
ROCKSUTIL = $(ROCKSUTIL_PATH)/librocksutil$(DEBUG_SUFFIX).a

INCLUDE_PATH = -I./ \
							 -I$(SLASH_PATH)/ \
							 -I$(PINK_PATH)/ \
							 -I$(FLOYD_PATH)/ \
							 -I$(ROCKSDB_PATH)/ \
							 -I$(ROCKSUTIL_PATH)/ \
							 -I$(ROCKSUTIL_PATH)/include 

LIB_PATH = -L./ \
					 -L$(FLOYD_PATH)/floyd/lib/ \
					 -L$(SLASH_PATH)/slash/lib/ \
					 -L$(PINK_PATH)/pink/lib/ \
					 -L$(ROCKSDB_PATH) \
					 -L$(ROCKSUTIL_PATH)

LDFLAGS += $(LIB_PATH) \
					 -lprotobuf \
			 		 -lfloyd$(DEBUG_SUFFIX) \
			 		 -lpink$(DEBUG_SUFFIX) \
			 		 -lslash$(DEBUG_SUFFIX) \
			 		 -lz \
			 		 -lbz2 \
			 		 -lrocksdb \
			 		 -lsnappy \
			 		 -lrocksutil$(DEBUG_SUFFIX)

# ---------------End Dependences----------------

VERSION_CC=$(SRC_PATH)/build_version.cc
LIB_SOURCES :=  $(VERSION_CC) \
				$(filter-out $(VERSION_CC), $(wildcard $(SRC_PATH)/*.cc))


#-----------------------------------------------

AM_DEFAULT_VERBOSITY = 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $(notdir $@);
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $(notdir $@);
am__v_CC_1 =
CCLD = $(CC)
LINK = $(CCLD) $(AM_CFLAGS) $(CFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
am__v_CCLD_0 = @echo "  CCLD    " $(notdir $@);
am__v_CCLD_1 =

AM_LINK = $(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

CXXFLAGS += -g

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  							-Wno-unused-parameter -Woverloaded-virtual \
								-Wnon-virtual-dtor -Wno-missing-field-initializers

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) $(INCLUDE_PATH) $(PLATFORM_CXXFLAGS) $(OPT)

LDFLAGS += $(PLATFORM_LDFLAGS)

date := $(shell date +%F)
git_sha := $(shell git rev-parse HEAD 2>/dev/null)
gen_build_version = sed -e s/@@GIT_SHA@@/$(git_sha)/ -e s/@@GIT_DATE_TIME@@/$(date)/ src/build_version.cc.in
# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
CLEAN_FILES += $(SRC_PATH)/build_version.cc

$(SRC_PATH)/build_version.cc: FORCE
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@-t
	$(AM_V_at)if test -f $@; then         \
	  cmp -s $@-t $@ && rm -f $@-t || mv -f $@-t $@;    \
	else mv -f $@-t $@; fi
FORCE: 

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

# if user didn't config LIBNAME, set the default
ifeq ($(BINNAME),)
# we should only run pika_hub in production with DEBUG_LEVEL 0
BINNAME=pika_hub$(DEBUG_SUFFIX)
endif
BINARY = ${BINNAME}

.PHONY: distclean clean dbg all

%.o: %.cc
	  $(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

%.d: %.cc
	@set -e; rm -f $@; $(CXX) -MM $(CXXFLAGS) $< > $@.$$$$; \
	sed 's,\($(notdir $*)\)\.o[ :]*,$(SRC_DIR)/\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifneq ($(MAKECMDGOALS),clean)
  -include $(LIBOBJECTS:.o=.d)
endif


all: $(BINARY)

dbg: $(BINARY)

$(BINARY): $(FLOYD) $(PINK) $(SLASH) $(ROCKSUTIL) $(LIBOBJECTS)
	$(AM_V_at)rm -f $@
	$(AM_V_at)$(AM_LINK)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)mkdir -p $(OUTPUT)/bin
	$(AM_V_at)mv $@ $(OUTPUT)/bin
	$(AM_V_at)cp -r $(CURDIR)/conf $(OUTPUT)
	

$(FLOYD):
	$(AM_V_at)make -C $(FLOYD_PATH)/floyd/ DEBUG_LEVEL=$(DEBUG_LEVEL) SLASH_PATH=$(SLASH_PATH) PINK_PATH=$(PINK_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH)

$(PINK):
	$(AM_V_at)make -C $(PINK_PATH)/pink/ DEBUG_LEVEL=$(DEBUG_LEVEL)  SLASH_PATH=$(SLASH_PATH)

$(SLASH):
	$(AM_V_at)make -C $(SLASH_PATH)/slash/ DEBUG_LEVEL=$(DEBUG_LEVEL)

$(ROCKSUTIL):
	$(AM_V_at)make -C $(ROCKSUTIL_PATH) DEBUG_LEVEL=$(DEBUG_LEVEL)

$(ROCKSDB):
	$(AM_V_at)make -C $(ROCKSDB_PATH)/ static_lib DEBUG_LEVEL=$(DEBUG_LEVEL)

clean:
	rm -f $(BINARY)
	rm -rf $(CLEAN_FILES)
	find $(SRC_PATH) -name "*.[oda]*" -exec rm -f {} \;
	find $(SRC_PATH) -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

distclean: clean
	make -C $(FLOYD_PATH)/floyd clean
	make -C $(PINK_PATH)/pink/ clean
	make -C $(SLASH_PATH)/slash/ clean
	make -C $(ROCKSUTIL_PATH) clean
#	make -C $(ROCKSDB_PATH)/ clean
