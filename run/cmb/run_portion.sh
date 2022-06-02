#!/bin/bash

portion=2
start=1000000
end=2000000

num="_5M"   # MODIFT
SSD_PATH="/media/nvme"  # MODIFT

./cmb_btree.out -i ../../data/insert$num.txt -r $start $end $SSD_PATH/tree
cp $SSD_PATH/tree checkpoint/tree_$portion
mv insert$num.dat checkpoint/insert$num"_"$portion.dat
mv tree.dat checkpoint/tree_$portion.dat
./dup_cmb.out fake_cmb c
cp fake_cmb checkpoint/cmb_$portion
chmod 444 checkpoint/*
