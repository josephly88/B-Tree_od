#!/bin/bash

num="_1k"   # MODIFT
SSD_PATH="/mnt/c/Users/s1155095176/Documents/GitHub/fake_SSD"  # MODIFT
RESULT_PATH="/home/meteor/GitHub/Result/fake_phase/copy_on_write"  # MODIFT

INSERT='insert'$num
LIST=('A'$num 'B'$num 'C'$num 'D'$num 'F'$num 'delete'$num)

rm -f $SSD_PATH/tree

./cpw_btree.out -i ../../data/$INSERT.txt $SSD_PATH/tree
../verify.py $INSERT.dat
cp $SSD_PATH/tree inst_tree
cp $INSERT.dat $RESULT_PATH
rm expected.dat tree.dat

for FILE in ${LIST[*]};
do
    cp inst_tree $SSD_PATH/tree
    ./cpw_btree.out -i ../../data/$FILE.txt $SSD_PATH/tree
    ../verify.py $INSERT.dat $FILE.dat
    mv $FILE.dat $RESULT_PATH
    rm -f expected.dat tree.dat
done

rm *.dat inst_tree