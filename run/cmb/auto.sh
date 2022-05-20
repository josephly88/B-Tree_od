#!/bin/bash

INSERT='t_insert'
LIST=('t_A' 't_B' 't_C' 't_D' 't_F' 't_delete')

for FILE in ${LIST[*]};
do
    rm $INSERT.dat
    ./cmb_btree.out -i ../../data/$INSERT.txt tree
    ../verify.py $INSERT.dat
    ./cmb_btree.out -i ../../data/$FILE.txt tree
    ../verify.py $INSERT.dat $FILE.dat
    rm -f expected.dat tree.dat tree
done
