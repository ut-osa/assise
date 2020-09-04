#! /usr/bin/sudo /bin/bash

numactl -N0 -m0 ../../libfs/bin/mkfs.mlfs 1
#numactl -N0 -m0 ../../libfs/bin/mkfs.mlfs 2
#numactl -N0 -m0 ../../libfs/bin/mkfs.mlfs 3
#numactl -N0 -m0 ../../libfs/bin/mkfs.mlfs 4
#numactl -N1 -m1 ../../libfs/bin/mkfs.mlfs 5
