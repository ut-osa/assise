CC = gcc -std=c99
#CC = c99
EXE = create_files iobench iobench_lat dmabench dmabench_lat
#$(info $(EXE))

CUR_DIR = $(shell pwd)
PROJECT_ROOT = $(CUR_DIR)/../../
LIBFS_DIR = $(abspath $(PROJECT_ROOT)/libfs/build/)
KERNFS_DIR = $(abspath $(PROJECT_ROOT)/kernfs/)

NVML_DIR = $(abspath $(LIBFS_DIR)/../lib/nvml/src/nondebug/)
LIBFS = $(LIBFS_DIR)/libmlfs.a
RDMA_DIR := $(abspath $(PROJECT_ROOT)/libfs/lib/rdma)

SPDK_ROOT_DIR := $(abspath $(KERNFS_DIR)/lib/spdk)
SPDK_ROOT_INC := $(abspath $(KERNFS_DIR)/lib/spdk/include/)

INCLUDES= $(LIBFS_DIR)/../src -I$(CUR_DIR)/../lib/nvml/src/include -I$(SPDK_ROOT_DIR)

CFLAGS= -I$(INCLUDES)

include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SPDK_LIBS += $(SPDK_ROOT_DIR)/build/lib/libspdk_ioat.a \
			 $(SPDK_ROOT_DIR)/build/lib/libspdk_util.a \
			 $(SPDK_ROOT_DIR)/build/lib/libspdk_log.a \
			 $(ENV_LIBS)

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk



LDFLAGS =	  -Wl,-rpath=$(abspath $(LIBFS_DIR)) \
		  -Wl,-rpath=$(abspath $(SPDK_ROOT_DIR)) \
		  -Wl,-rpath=$(abspath $(NVML_DIR)) \
  		  -Wl,-rpath=/usr/lib64 \
		  -Wl,-rpath=/usr/local/lib \
		  -Wl,-rpath=/usr/lib/x86_64-linux-gnu/ \
		  -Wl,-rpath=/lib/x86_64-linux-gnu/ \
		  -lpthread -lrt -lm -lssl -lcrypto -laio

all: $(EXE)

%.o: %.c
	$(CC) $(CFLAGS) -c -g $< -o $@

%.o: %.cc
	$(CXX) -std=c++11 $(CFLAGS) -c -g $< -o $@

time_stat.o: time_stat.c
	$(CC) time_stat.c -c -o time_stat.o -D_BSD_SOURCE

create_files: create_files.cc
	$(CXX) -std=c++11 -g -Ofast -o $@ $^  -I$(INCLUDES) -L$(LIBFS_DIR) -lmlfs $(CFLAGS) -L$(RDMA_DIR) $(LDFLAGS)

iobench: iobench.cc time_stat.o thread.cc
	$(CXX) -std=c++11 -g -Ofast -o $@ $^  -I$(INCLUDES) -L$(LIBFS_DIR) -lmlfs $(CFLAGS) -L$(RDMA_DIR) $(LDFLAGS)

iobench_lat: iobench_lat.cc time_stat.o thread.cc
	$(CXX) -std=c++11 -g -Ofast -o $@ $^  -I$(INCLUDES) -L$(LIBFS_DIR) -lmlfs $(CFLAGS) -L$(RDMA_DIR) $(LDFLAGS) $(MISC_FLAGS) 

dmabench: dmabench.cc time_stat.o thread.cc
	$(CXX) -std=c++11 -Ofast -g -o $@ $^ $(CFLAGS) -Wl,--whole-archive $(SPDK_LIBS) -Wl,--no-whole-archive -L$(NVML_DIR) -ldl -luuid -lnuma -lpmem -lpthread -lm -lrt -Wl,-rpath=$(abspath $(NVML_DIR)) -I$(INCLUDES) $(LDFLAGS)

dmabench_lat: dmabench.cc time_stat.o thread.cc
	$(CXX) -std=c++11 -Ofast -g -o $@ $^ $(CFLAGS) -Wl,--whole-archive $(SPDK_LIBS) -Wl,--no-whole-archive -L$(NVML_DIR) -ldl -luuid -lnuma -lpmem -lpthread -lm -lrt -Wl,-rpath=$(abspath $(NVML_DIR)) -I$(INCLUDES) $(LDFLAGS) -DLAT_BENCH

strawman: time_stat.c thread.cc
	gcc time_stat.c -c -std=c99 -D_BSD_SOURCE -O2
	g++ -g -c iobench.cc -std=c++11 -lpthread -O2 -DUSER_BLOCK_MIGRATION
	g++ thread.cc -c -std=c++11 -O2
	#gcc aligned_block_migration.c -c
	g++ batch_migration.cc -c -std=c++11 -O2
	g++ migration_manager.cc -c -std=c++11 -msse4.1 -mavx -O2 -g
	g++ iobench.o thread.o batch_migration.o time_stat.o migration_manager.o -lpthread -std=c++11 -g -O2

clean:
	rm -rf *.o $(EXE) *.normal

