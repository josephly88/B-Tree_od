#!/bin/bash

portion=9_2
start=9500000
end=10000000

num="_5M"   # MODIFT
SSD_PATH="/media/nvme"  # MODIFT

workload="read_and_update"

./cpw_btree.out -i ../../data/$workload$num.txt -r $start $end $SSD_PATH/tree
cp $SSD_PATH/tree checkpoint/tree_$portion
mv $workload$num.dat checkpoint/$workload$num"_"$portion.dat
mv tree.dat checkpoint/tree_$portion.dat
chmod 444 checkpoint/*
