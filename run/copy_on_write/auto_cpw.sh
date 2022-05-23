#!/bin/bash

num="_10M"   # MODIFT
SSD_PATH="/media/nvme"  # MODIFT
RESULT_PATH="/home/meteor/Documents/Result/10M/copy_on_write"  # MODIFT

INSERT='insert'$num
LIST=('read_and_update'$num 'delete'$num)

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

mv inst_tree $RESULT_PATH/..
rm *.dat
rm $SSD_PATH/tree
