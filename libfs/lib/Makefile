DPDK_VER := 16.11

.PHONY: all download redownload clean
all:	download
	#if [ ! -d "dpdk-$(DPDK_VER)" ]; then tar xf dpdk-$(DPDK_VER).tar.xz; fi
	#sudo make install -C dpdk-$(DPDK_VER) T=x86_64-native-linuxapp-gcc DESTDIR=dpdk-$(DPDK_VER)/ -j8
	make -C nvml -j8
	(cd rdma-core && ./build.sh)
	(cd rdma && make)
	(cd syscall_intercept; \
	 mkdir build; \
	 mkdir install; \
	 cd build; \
	 cmake -DCMAKE_INSTALL_PREFIX=$(shell pwd)/syscall_intercept/install -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_BUILD_TYPE=Release ..; \
	 make; \
	 make install)
	if [ ! -d jemalloc-4.5.0 ]; then tar -xf jemalloc-4.5.0.tar.bz2; fi
	(cd jemalloc-4.5.0 && \
	 ./autogen.sh && \
	 ./configure && \
	 make && \
	 sudo make install)

redownload: REPO_PURGE=1
redownload: download

#shouldn't download DPDK, there is an issue right now with fPIC; it's manually
#added in its Makefile (mk/rte.lib.mk, line 53 has CFLAGS += -fPIC).
#DPDK has a config to make it shared (CONFIG_RTE_BUILD_SHARED_LIB=y)
#but it breaks SPDK compilation
download:
	########
	# 	NVML
	########
	if [ ! -d "nvml" ] || [ ! -z $(REPO_PURGE) ]; then\
		rm -rf nvml;\
		git clone https://github.com/pmem/nvml;\
		(cd nvml && git checkout tags/1.7);\
	fi
	########
	#       syscall_intercept
	########
	if [ ! -d "syscall_intercept" ] || [ ! -z $(REPO_PURGE) ]; then\
		rm -rf syscall_intercept;\
		git clone https://github.com/pmem/syscall_intercept.git;\
	fi

clean:
	@rm -rf dpdk-$(DPDK_VER)/x86_64-native-linuxapp-gcc
	make clean -C nvml
	@rm -rf rdma-core/build
