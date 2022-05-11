#!/bin/bash

LIST=('A' 'B' 'C' 'D' 'F' 'delete')

for i in {0..5}
do
    rm insert.dat
    ./cmb_btree.out -i ../../data/insert.txt tree
    ../verify.py insert.dat
    ./cmb_btree.out -i ../../data/${LIST[i]}.txt tree
    ../verify.py insert.dat ${LIST[i]}.dat
    rm -f expected.dat tree.dat tree
done
