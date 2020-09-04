#!/bin/bash

if [  -n "$(uname -a | grep Ubuntu)" ]; then
	# apt update
	#sudo apt update

	echo "Installing required packages.."
	sudo apt-get install -y build-essential
	sudo apt-get install -y make pkg-config autoconf
	sudo apt-get install -y ndctl
	sudo apt-get install -y libnuma-dev libaio1 libaio-dev uuid-dev librdmacm-dev numactl
	sudo apt-get install -y libncurses-dev libssl-dev libelf-dev rsync

else
	echo "Non-Ubuntu OS detected. You may need to install required packages manually (refer to dependencies in README).\n"
fi

echo "Building dependent Assise libraries (RDMA-CORE, NVML, JEMALLOC, syscall-intercept).."
cd ../libfs/lib; sudo make; cd ../../deps


