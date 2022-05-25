#!/bin/bash

num="_200K"   # MODIFT

DIR=`pwd`
YCSB_PATH="/home/meteor/Documents/YCSB/"  # MODIFY
RESULT_PATH="/home/meteor/Documents/Result/10M/" # MODIFY

YCSB() {
    cd $YCSB_PATH
    ./bin/ycsb load basic -P $DIR/data/workloadc > $DIR/data/insert$num.txt
    ./bin/ycsb run basic -P $DIR/data/workloadc > $DIR/data/C$num.txt
    ./bin/ycsb run basic -P $DIR/data/workload_read_update > $DIR/data/read_and_update$num.txt
    cd $DIR

    ./data/delete_gen.sh ./data/C$num.txt > ./data/delete$num.txt
}

YCSB
