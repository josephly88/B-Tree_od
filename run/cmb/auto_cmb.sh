#!/bin/bash

num="_1k"  # MODIFT
SSD_PATH="/mnt/c/Users/s1155095176/Documents/GitHub/fake_SSD"  # MODIFT
RESULT_PATH="/home/meteor/GitHub/Result/fake_phase/cmb"  # MODIFT

INSERT='insert'$num
LIST=('A'$num 'B'$num 'C'$num 'D'$num 'F'$num 'delete'$num)

rm -f $SSD_PATH/tree

./cmb_btree.out -i ../../data/$INSERT.txt $SSD_PATH/tree
../verify.py $INSERT.dat
mv $INSERT.dat $RESULT_PATH
rm *.dat

for FILE in ${LIST[*]};
do
    rm $SSD_PATH/tree
    ./cmb_btree.out -i ../../data/$INSERT.txt $SSD_PATH/tree
    ../verify.py $INSERT.dat
    ./cmb_btree.out -i ../../data/$FILE.txt $SSD_PATH/tree
    ../verify.py $INSERT.dat $FILE.dat
    mv $FILE.dat $RESULT_PATH
    rm -f *.dat
done