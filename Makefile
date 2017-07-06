RPATH = /usr/local/pika22/
LFLAGS = -Wl,-rpath=$(RPATH)

UNAME := $(shell if [ -f "/etc/redhat-release" ]; then echo "CentOS"; else echo "Ubuntu"; fi)

OSVERSION := $(shell cat /etc/redhat-release | cut -d "." -f 1 | awk '{print $$NF}')

ifeq ($(UNAME), Ubuntu)
  SO_PATH = $(CURDIR)/lib/ubuntu
else ifeq ($(OSVERSION), 5)
  SO_PATH = $(CURDIR)/lib/5.4
else
  SO_PATH = $(CURDIR)/lib/6.2
endif

CXX = g++

ifeq ($(__PERF), 1)
#CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
	CXXFLAGS = -O0 -g -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -O2 -g -gstabs+ -pg -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -DROCKSUTIL_PTHREAD_ADAPTIVE_MUTEX
endif

SRC_PATH = ./src/
THIRD_PATH = ./third
OUTPUT = ./output
VERSION = -D_GITVER_=$(shell git rev-list HEAD | head -n1) \
					-D_COMPILEDATE_=$(shell date +%F)

ifndef SLASH_PATH
SLASH_PATH = $(realpath $(THIRD_PATH)/slash)
endif

ifndef PINK_PATH
PINK_PATH = $(realpath $(THIRD_PATH)/pink)
endif

ifndef NEMODB_PATH
NEMODB_PATH = $(realpath $(THIRD_PATH)/nemo-rocksdb)
endif

ifndef FLOYD_PATH
FLOYD_PATH = $(realpath $(THIRD_PATH)/floyd)
endif

ROCKSUTIL_PATH = $(realpath $(THIRD_PATH)/rocksutil)

SRC = $(wildcard $(SRC_PATH)/*.cc)
OBJS = $(patsubst %.cc,%.o,$(SRC))


PIKA_HUB = pika_hub


INCLUDE_PATH = -I./ \
							 -I$(SLASH_PATH)/ \
							 -I$(PINK_PATH)/ \
							 -I$(FLOYD_PATH)/ \
							 -I$(NEMODB_PATH)/ \
							 -I$(NEMODB_PATH)/rocksdb \
							 -I$(NEMODB_PATH)/rocksdb/include \
							 -I$(ROCKSUTIL_PATH)/ \
							 -I$(ROCKSUTIL_PATH)/include 

LIB_PATH = -L./ \
					 -L$(FLOYD_PATH)/floyd/lib/ \
					 -L$(SLASH_PATH)/slash/lib/ \
					 -L$(PINK_PATH)/pink/lib/ \
					 -L$(NEMODB_PATH)/output/lib/ \
					 -L$(ROCKSUTIL_PATH)

LIBS = -lpthread \
			 -lprotobuf \
			 -lfloyd \
			 -lpink \
			 -lslash \
			 -lz \
			 -lbz2 \
			 -lnemodb \
			 -lrocksdb \
			 -lsnappy \
			 -lrt \
			 -lcrypto \
			 -lssl \
			 -lrocksutil

FLOYD = $(FLOYD_PATH)/floyd/lib/libfloyd.a
NEMODB = $(NEMODB_PATH)/output/lib/libnemodb.a
PINK = $(PINK_PATH)/pink/lib/libpink.a
SLASH = $(SLASH_PATH)/slash/lib/libslash.a
ROCKSUTIL = $(ROCKSUTIL_PATH)/rocksutil/librocksutil.a

.PHONY: all clean distclean


all: $(PIKA_HUB)
	@echo "OBJS $(OBJS)"
	echo "PINK_PATH $(PINK_PATH)"
	echo "SLASH_PATH $(SLASH_PATH)"
	echo "FLOYD_PATH $(FLOYD_PATH)"
	echo "NEMODB_PATH $(NEMODB_PATH)"
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	mkdir $(OUTPUT)/lib
#	cp -r $(SO_PATH)/*  $(OUTPUT)/lib
	mv $(PIKA_HUB) $(OUTPUT)/bin/
	cp -r conf $(OUTPUT)
	@echo "Success, go, go, go..."


$(PIKA_HUB): $(FLOYD) $(PINK) $(SLASH) $(ROCKSUTIL) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) $(VERSION)

$(FLOYD):
	make -C $(FLOYD_PATH)/floyd/ __PERF=$(__PERF) SLASH_PATH=$(SLASH_PATH) PINK_PATH=$(PINK_PATH) NEMODB_PATH=$(NEMODB_PATH)

$(NEMODB):
	make -C $(NEMODB_PATH)/

$(SLASH):
	make -C $(SLASH_PATH)/slash/ __PERF=$(__PERF)

$(PINK):
	make -C $(PINK_PATH)/pink/ __PERF=$(__PERF)  SLASH_PATH=$(SLASH_PATH)

$(ROCKSUTIL):
	make -C $(ROCKSUTIL_PATH)

clean: 
	rm -rf $(SRC_PATH)/*.o
	rm -rf $(OUTPUT)

distclean: clean
	make -C $(PINK_PATH)/pink/ clean
	make -C $(SLASH_PATH)/slash/ clean
	make -C $(NEMODB_PATH)/ clean
	make -C $(FLOYD_PATH)/floyd/ clean
	make -C $(ROCKSUTIL_PATH) clean

