#!/bin/bash

num="_1k"   # MODIFT

DIR=`pwd`
YCSB_PATH="/home/meteor/YCSB/"  # MODIFY
RESULT_PATH="/home/meteor/GitHub/Result/fake_phase/" # MODIFY

cd $YCSB_PATH
./bin/ycsb load basic -P workloads/workloada -P $DIR/data/property_ycsb > $DIR/data/insert$num.txt
./bin/ycsb run basic -P workloads/workloada -P $DIR/data/property_ycsb > $DIR/data/A$num.txt
./bin/ycsb run basic -P workloads/workloadb -P $DIR/data/property_ycsb > $DIR/data/B$num.txt
./bin/ycsb run basic -P workloads/workloadc -P $DIR/data/property_ycsb > $DIR/data/C$num.txt
./bin/ycsb run basic -P workloads/workloadd -P $DIR/data/property_ycsb > $DIR/data/D$num.txt
./bin/ycsb run basic -P workloads/workloadf -P $DIR/data/property_ycsb > $DIR/data/F$num.txt
cd $DIR

./data/delete_gen.sh ./data/C$num.txt > ./data/delete$num.txt

cd run/copy_on_write
./auto_cpw.sh

cd ../cmb
./auto_cmb.sh

cd $DIR
cp plot.py $RESULT_PATH
cd $RESULT_PATH
./plot.py 100
rm plot.py
cd $DIR