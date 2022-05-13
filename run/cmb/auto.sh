#!/bin/bash

LIST=('A' 'B' 'C' 'D' 'F' 'delete')

for FILE in $LIST
do
    rm insert.dat
    ./cmb_btree.out -i ../../data/insert.txt tree
    ../verify.py insert.dat
    ./cmb_btree.out -i ../../data/$FILE.txt tree
    ../verify.py insert.dat $FILE.dat
    rm -f expected.dat tree.dat tree
done
