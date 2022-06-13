#!/bin/bash

portion=1_1
start=0000000
end=5000000

num="_5M"   # MODIFT
SSD_PATH="/media/nvme"  # MODIFT

workload="insert"

./cmb_btree.out -i ../../data/$workload$num.txt -r $start $end $SSD_PATH/tree
cp $SSD_PATH/tree checkpoint/tree_$portion
mv $workload$num.dat checkpoint/$workload$num"_"$portion.dat
mv tree.dat checkpoint/tree_$portion.dat
./copy_cmb.out fake_cmb c
cp fake_cmb checkpoint/cmb_$portion
chmod 444 checkpoint/*
