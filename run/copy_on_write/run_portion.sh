#!/bin/bash

portion=5_2
start=4500000
end=5000000

num="_5M"   # MODIFT
SSD_PATH="/media/nvme"  # MODIFT

workload="delete"

./cpw_btree.out -i ../../data/$workload$num.txt -r $start $end $SSD_PATH/tree
cp $SSD_PATH/tree checkpoint/tree_$portion
mv $workload$num.dat checkpoint/$workload$num"_"$portion.dat
mv tree.dat checkpoint/tree_$portion.dat
chmod 444 checkpoint/*
