#!/bin/bash

portion=5
start=4000000
end=5000000

num="_5M"   # MODIFT
SSD_PATH="/media/nvme"  # MODIFT

./cpw_btree.out -i ../../data/insert$num.txt -r $start $end $SSD_PATH/tree
cp $SSD_PATH/tree checkpoint/tree_$portion
mv insert$num.dat checkpoint/insert$num"_"$portion.dat
mv tree.dat checkpoint/tree_$portion.dat
