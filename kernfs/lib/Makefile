
.PHONY: all download redownload clean
all:    download
	#make install -C dpdk-$(DPDK_VER) T=x86_64-native-linuxapp-gcc DESTDIR=dpdk-$(DPDK_VER)/ -j8
	(cd spdk && \
	 git submodule update --init && \
	 sudo ./scripts/pkgdep.sh && \
	 ./configure && \
	 make -j8)

download:
	########
	# 	SPDK
	########
	if [ ! -d "spdk" ] || [ ! -z $(REPO_PURGE) ]; then\
		rm -rf spdk;\
		git clone https://github.com/spdk/spdk.git;\
	fi

redownload: REPO_PURGE=1
redownload: download

clean:
	make clean -C spdk
