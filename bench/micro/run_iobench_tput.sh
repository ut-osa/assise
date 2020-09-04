# Used for running throughput experiments

[ -f output.log ] && rm output.log
[ -f temp.log ] && rm temp.log

PROCS=$4

if [ "$#" -ne 4 ]; then
    ./iobench -h
	exit
fi

if [ "$1" = "sw" ] || [ "$1" = "rw" ]; then
    sudo ./run.sh create_files $PROCS
    sleep 1
fi

for run in $(seq 1 $PROCS)
do
        #FILE_ID=$run numactl -N0 -m0 ./run.sh iobench rr 128M 64K 1 >> output.log &
        #FILE_ID=$run numactl -N0 -m0 ./run.sh iobench sr 512M 64K 1 &
        sudo FILE_ID=$run MLFS_DIGEST_TH=30 numactl -N0 -m0 ./run.sh iobench $1 $2 $3 1 -p >> output.log &
        #echo 'df'
done

sleep 5

sudo ./iobench -p >> temp.log

sleep 15

number=$(grep -oP '(?<=throughput: )[0-9]+' output.log | sort -n | tail -1)

#if [ "$1" == "sw" ]; then
#    echo "Sequential Write Throughput (MB/s):"
#else
#    echo "Sequential Read Throughput (MB/s):"
#fi

case $1 in
        ("sw") echo "Sequential Write Throughput (MB/s):" ;;
        ("rw") echo "Random Write Throughput (MB/s):" ;;
    	("sr") echo "Sequential Read Throughput (MB/s):" ;;
    	("rr") echo "Random Read Throughput (MB/s):" ;;
        (*)   echo "Unknown workload";;
esac


echo "$PROCS*$number" | bc -l

#sleep 1

#[ -f output.log ] && rm output.log
#[ -f temp.log ] && rm temp.log

