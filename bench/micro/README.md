
## Microbenchmarks

This directory contains our microbenchmarking scripts.

### Filesystem performance

*iobench* and *iobench_lat* can be used to benchmark filesystem throughput and latency, respectively. Refer to their usage manual for more instructions.

#### 1. Measuring Latency

We describe below how to measure unladen write and read latencies.

##### 1.1 Writes #####

For write latencies, we disable digestion as it can impact IO performance. To do so, you can run:

```
MLFS_DIGEST_TH=100 ./run.sh iobench sw 256M 4K 1 # sequential write
```

MLFS_DIGEST_TH is a %-based digest threshold based on log occupancy. Setting to 100 prevents Assise from pre-emptively digesting data. Before running this script, make sure that your log size is at least 1 GB in order to accommodate the benchmark's workload.

##### 1.2 Reads #####

Cache miss reads can be performed as follows:

```
./run.sh iobench_lat sr 256M 4K 1 # sequential read
```

To measure the latency for cache hits, you can write data to the log and then subsequently read them (but without digesting). To do so, similarly to the earlier write benchmark, you can run:

```
MLFS_DIGEST_TH=100 ./run.sh iobench_lat wr 256M 4K 1 

```

The *wr* argument specifies a sequential write-read workload.

#### 2. Measuring Throughput

To evaluate the system's throughput, you can use the convenience script *run_iobench_tput.sh*. This script generates a LibFS instance per each iobench thread in order to remove software bottlenecks and allow Assise to scale freely.

### DMA and Load/Store performance

*dmabench* & *dmabench_lat* are dma benchmarking utilities that work with DRAM/NVM and used for measuring throughput and latency, respectively.



