#!/bin/bash

name="native"

ARG=""
#ARG="-cmb"
#ARG="-cmb -append 8"

[ -d "email" ] && rm email

workload="load"
echo "#############################################################" | tee email
echo "LOAD" | tee -a email

./merge_btree.out -new $ARG -input ../../data/ycsb/${workload}${dist}.txt /dev/nvme0n1 
mv tree.dat ${name}_${workload}${size}_tree.dat

./copy_tree.out dup_tree c
cp dup_tree ${name}_${workload}${dist}_tree
mv ${name}_${workload}${dist}_tree  Loaded/

./copy_cmb.out dup_cmb c
cp dup_cmb ${name}_${workload}${dist}_cmb
mv ${name}_${workload}${dist}_cmb Loaded/

echo " Done " | tee -a email
echo "#############################################################" | tee -a email
cat email | mail -s "LOAD Finish" josephly88@yahoo.com.hk
rm email

workload=("workloada" "workloadb" "workloadf")
dist=("_uni" "_zip")
for WL in "${workload[@]}"
do
    for DST in "${dist[@]}"
    do
        echo "#############################################################" | tee email
        echo "YCSB $WL$DST" | tee -a email

        ./copy_tree.out Loaded/${name}_load_tree p
        ./copy_cmb.out Loaded/${name}_load_cmb p

        ./merge_btree.out $ARG -input ../../data/ycsb/${WL}${DST}.txt /dev/nvme0n1 
        mv tree.dat ${name}_${WL}${DST}_tree.dat

        echo " Done " | tee -a email
        echo "#############################################################" | tee -a email
        cat email | mail -s "YCSB $WL$DST Finish" josephly88@yahoo.com.hk
        rm email
    done
done

WL="workloadd"
DST=""
echo "#############################################################" | tee email
echo "YCSB $WL$DST" | tee -a email

./copy_tree.out Loaded/${name}_load_tree p
./copy_cmb.out Loaded/${name}_load_cmb p

./merge_btree.out $ARG -input ../../data/ycsb/${WL}${DST}.txt /dev/nvme0n1 
mv tree.dat ${name}_${WL}${DST}_tree.dat

echo " Done " | tee -a email
echo "#############################################################" | tee -a email
cat email | mail -s "YCSB $WL$DST Finish" josephly88@yahoo.com.hk
rm email
