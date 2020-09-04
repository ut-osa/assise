#! /bin/bash

#benchmark=fillseq
#benchmark=readrandom


function show_usage() {
    echo "./run_bench.sh <workload>"
    echo "workload types: readseq, randrandom, readhot, fillseq, fillrandom, fillsync"
    exit
}

if [ $# -eq 0 ]
then
    show_usage
fi

WORKLOAD=""

if [[ "$1" == *read* ]]; then
    WORKLOAD="fillseq,"
fi

WORKLOAD+=$1

#./run.sh ./db_bench.mlfs --db=/mlfs --num=400000 --value_size=1024 --benchmarks=fillseq,readseq
#./run.sh ./db_bench.mlfs --db=/mlfs --num=400000 --value_size=1024 --benchmarks=fillseq,fillrandom,readseq,readrandom,readhot
#./run.sh ./db_bench.mlfs --db=/mlfs --num=400000 --value_size=1024 --benchmarks=fillseq,readhot
#./run.sh ./db_bench.mlfs --db=/mlfs --num=400000 --value_size=1024 --benchmarks=fillseq
./run.sh ./db_bench.mlfs --db=/mlfs --num=400000 --value_size=1024 --benchmarks=$WORKLOAD
#./run.sh ./db_bench.mlfs --db=/mlfs --num=400000 --value_size=1024 --benchmarks=fillsync

