Assise
===========================

Assise is a distributed file system designed to maximize the use of client-local NVM while providing linearizability for IO operations. Assise unleashes the performance of NVM via pervasive and persistent caching in process-local, socket-local, and node-local NVM. Assise accelerates POSIX file IO by orders of magnitude by leveraging client-local NVM without kernel involvement, block amplification, or unnecessary coherence overheads.

Assise uses a userspace file system library -- LibFS -- that intercepts POSIX calls and turns them into function calls, allowing reads and writes to be served using loads and stores to local NVM caches. We use a daemon called SharedFS to coordinate linearizable state among LibFS processes via leases. Our filesystem provides near-instantaneous application fail-over onto a hot replica that mirrors an application local file system cache in the replica.

Our [OSDI paper](https://wreda.github.io/papers/assise-osdi20.pdf) describes our system in detail.

To cite us, you can use the following BibTex entry:
```
@inproceedings {assise,
title = {Assise: Performance and Availability via Client-local {NVM} in a Distributed File System},
author={Anderson, Thomas E and Canini, Marco and Kim, Jongyul and Kosti{\'c}, Dejan and Kwon, Youngjin and Peter, Simon and Reda, Waleed and Schuh, Henry N and Witchel, Emmett},
booktitle = {14th {USENIX} Symposium on Operating Systems Design and Implementation ({OSDI} 20)},
year = {2020},
url = {https://www.usenix.org/conference/osdi20/presentation/anderson},
publisher = {{USENIX} Association},
month = nov,
}
```

## System Requirements ##

1. Ubuntu 16.04 / 18.04 / Fedora 27
2. Linux kernel version 4.2+
3. 8 or more GB of DRAM.
4. 4 or more CPU cores.

## Getting Started ##
Assume the current directory is the project's root directory.

##### [1. Build & Install dependencies](#dependencies)

##### [2. Configure Storage](#storage-configuration)
    
##### [3. Configure Cluster](#cluster-configuration)

##### 4. Build Assise

LibFS:
~~~
cd libfs; make clean; make; cd tests; make clean; make; cd ../..
~~~

KernFS:
~~~
cd kernfs; make clean; make; cd tests; make clean; make; cd ../..
~~~

##### 5. Run MKFS
~~~
cd kernfs/tests; ./mkfs.sh; cd ../..
~~~

This will build your filesystem on all [configured devices](#storage-configuration)
Before formatting your devices, make sure their storage sizes are appropriately configured.

##### 6. Run KernFS

For each node in the cluster, do:
~~~
cd kernfs/tests
sudo ./run.sh kernfs
~~~

Make sure to run KernFS on the nodes in the reverse order of 'libfs/src/distributed/rpc_interface.h' to avoid connection
establishment issues.

##### 5. Run a test application
This test will write sequentially to a 2GB file using 4K IO and 1 thread
~~~
cd libfs/tests
sudo ./run.sh iotest sw 2G 4K 1
~~~

## Basic Usage ##

To run Assise with any application, you need to LD_PRELOAD LibFS. e.g.
~~~
LD_PRELOAD=./libfs/build/libmlfs.so sample_app
~~~

**Note:** Assise will only intercept supported filesystem calls that are accessing the default Assise directory '/mlfs'. This directory is statically defined in 'libfs/src/shim/shim.c' under 'MLFS_PREFIX'

## Storage Configuration ##

### Create DAX namespaces

To run Assise with NVM, you need to create DEV-DAX devices (aka namespaces) via either one of the following options:

####     a. Real NVM
If your machines are equipped with NVDIMMs, you can use the [ndctl utility](https://docs.pmem.io/ndctl-user-guide/quick-start).
The following command creates a device on socket 0 with a capacity of 32 GB:

~~~
ndctl create-namespace --region=region0 --mode=devdax --size=32G
~~~

####     b. Emulated NVM
Alternatively, you can use DRAM to emulate NVM. To do so, follow the instructions in our [emulation guide](https://github.com/ut-osa/assise/blob/master/docs/emulation.md) or refer to this [article](https://software.intel.com/content/www/us/en/develop/articles/how-to-emulate-persistent-memory-on-an-intel-architecture-server.html).
This will allocate DRAM devices (typically named '/mnt/pmemX'). To be usable by Assise, you need to convert them to DEV-DAX mode as follows:

~~~
cd utils
sudo ./use_dax.sh bind 
~~~

To undo this step, simply run:
~~~
sudo ./use_dax.sh unbind
~~~

### Configure size

This script sets the storage size reserved by Assise for each device. If using NVM only, set both SSD & HDD sizes to zero.

~~~
./utils/change_dev_size.py <NVM GB> <SSD GB/0> <HDD GB/0>
~~~

**Note:** By default, at least 30 GB of NVM capacity is required for log allocation. The remainder is assigned to the filesystem's shared cache. To tune these parameters, refer to the [params](#other-params) section.

## Cluster Configuration ##

To appropriately configure the cluster, consult the following files:

* (a) libfs/src/global/global.h
* (b) libfs/src/distributed/rpc_interface.h

For (a) set ‘g_n_hot_rep’ to the number of nodes you’re planning to use. By default, this value is 2.

For (b), configure the IP address of all the ‘g_n_hot_rep’ nodes in the hot_replicas array. Example for two-node cluster:

```c
static struct peer_id hot_replicas[g_n_hot_rep] = {                                                         
 { .ip = "172.17.15.2", .role = HOT_REPLICA, .type = KERNFS_PEER},                                   
 { .ip = "172.17.15.4", .role = HOT_REPLICA, .type = KERNFS_PEER},
};
```
### Running as a local filesystem ###
Assise can be configured to run locally. To do so, set ‘g_n_hot_rep’ to 1 and the address of KernFS to your localhost IP. In these settings, Assise behaves similarly to the local filesystem [Strata](https://github.com/ut-osa/strata) (which we use as a building block in our project).

## Dependencies ##

To build and install dependent packages:
~~~
cd deps; ./install_deps.sh; cd ..
~~~

This script installs dependent packages and downloads/builds varoius libraries --- including [syscall_intercept](https://github.com/pmem/syscall_intercept), which LibFS uses to intercept filesystem calls.

For non-Ubuntu systems, you may also need to manually install the following required packages:
~~~
build-essential make pkg-config autoconf cmake gcc libcapstone-dev libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind libnuma-dev libaio1 libaio-dev uuid-dev librdmacm-dev numactl ndctl libncurses-dev libssl-dev libelf-dev rsync libndctl-dev libdaxctl-dev
~~~

**Optional**
To build SPDK (only required for [Advanced](#flags) Assise configurations):
~~~
cd deps; ./build_spdk.sh; cd ..
~~~

To setup UIO for SPDK, run:
~~~
cd utils
sudo ./uio_setup.sh linux config
~~~
To rollback to previous setting,
~~~
sudo ./uio_setup.sh linux reset
~~~

For NVML and SPDK build errors, consult the appropriate [NVML](https://docs.pmem.io/persistent-memory/getting-started-guide/installing-pmdk/compiling-pmdk-from-source) and [SPDK](http://www.spdk.io/doc/getting_started.html) guides.

## Advanced Configuration ##

For more advanced configurations, we describe additional parameters.

**Note**: LibFS/KernFS should be recompiled following a configuration change.

### Flags ###

* **LibFS Makefile**
    * -DLAZY_SURRENDER: LibFS gives up leases 'lazily' by holding onto them until a revocation upcall by KernFS.
    * -DCC_OPT: Enable optimistic crash consistency.

* **KernFS Makefile**
    * -DENABLE_MEMCPY_OFFLOAD: KernFS delegates digestion of file data to onboard DMA engine. This config requires SPDK.
    * -DCONCURRENT: Allow KernFS to parallelize digests across LibFS processes.
    * -DFCONCURRENT: Allow KernFS to parallelize digests across different inodes. Note: Can be combined with -DCONCURRENT.
    * -DDIGEST_OPT: Enable log coalescing during digestion.
    
* **Both Makefiles**
    * -DISTRIBUTED: Use Assise in distributed mode (always keep this enabled; to-be-removed in the future).
    * -USE_LEASE : Enable leases for LibFS/KernFS.
    * -USE_SSD: Enable usage of an SSD.

**Note:** To change logging levels, use the -DMLFS_INFO, -DMLFS_DEBUG, or -DMLFS_LOG flags.
    
### Other params ###

* **'libfs/src/global/global.h'**
    * g_n_hot_rep: Number of hot replicas in the cluster.
    * g_log_size: Number of log blocks allocated per LibFS process.
    * g_n_max_libfs: Maximum number of LibFS processes allowed to run simultaneously.
    * g_max_read_cache_blocks: Maximum number of read cache blocks.
 
**Note:** (g_n_max_libfs + g_n_hot_rep) * g_log_size should be <= dev_size[g_log_dev].

## Troubleshooting ##


**mkfs.sh fails with ''mlfs_assert(file_size_blks > log_size_blks)''**

This indicates that the NVM device size defined during [step 2](#storage-configuration) is lower than the total log sizes. Either increase dev_size[g_log_dev] or reduce g_log_size.

##
**LibFS exits with ''not enough space for patching around syscal libsyscall_intercept error''**

This is a known problem with libsyscall and libc v2.30-1 (see: [link](https://github.com/pmem/syscall_intercept/issues/97)). To workaround this problem, please downgrade libc to v2.29-4 or below.

##
**LibFS exits with ''Unable to connect [error code: 111]''**

This indicates that KernFS is unreachable. Please make sure that KernFS is running and that its ports are not blocked by a firewall.
##

## License
This software is provided under the terms of the [GNU General Public License 2](https://www.gnu.org/licenses/gpl-2.0.html).

## Acknowledgements

This work is supported in part by the European Commission (EACEA) (FPA 2012-0030), ERC grant 770889, NSF grant CNS-1900457, and the Texas Systems Research Consortium. This work is also supported by the National Research Foundation of Korea (NRF) grant funded by the Korea government (MSIT) (2020R1C1C1014940). We thank Intel for access to the evaluation testbed.

## Contact

You can reach us at `assise@cs.utexas.edu` if you have any questions.
