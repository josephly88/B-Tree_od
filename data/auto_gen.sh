#!/bin/bash

num="_10K"   # MODIFT

DIR=`pwd`
YCSB_PATH="/home/meteor/YCSB"  # MODIFY

YCSB() {
    cd $YCSB_PATH
    ./bin/ycsb load basic -P $DIR/workloadc > $DIR/insert$num.txt
    ./bin/ycsb run basic -P $DIR/workloadc > $DIR/C$num.txt
    ./bin/ycsb run basic -P $DIR/workload_read_update > $DIR/read_and_update$num.txt
    cd $DIR

    ./delete_gen.sh ./C$num.txt > ./delete$num.txt
}

YCSB
