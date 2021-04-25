#!/bin/bash

if [  -n "$(uname -a | grep Ubuntu)" ]; then
	# apt update
	#sudo apt update

	echo "Installing required packages.."
	sudo apt-get install -y build-essential make pkg-config autoconf cmake gcc libcapstone-dev
	sudo apt-get install -y libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind
	sudo apt-get install -y libnuma-dev libaio1 libaio-dev uuid-dev librdmacm-dev numactl ndctl
	sudo apt-get install -y libncurses-dev libssl-dev libelf-dev rsync libndctl-dev libdaxctl-dev

else
	echo "Non-Ubuntu OS detected. You may need to install required packages manually (refer to dependencies in README).\n"
fi

echo "Building dependent Assise libraries (RDMA-CORE, NVML, JEMALLOC, syscall-intercept).."
cd ../libfs/lib; sudo make; cd ../../deps


