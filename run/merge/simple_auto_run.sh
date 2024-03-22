#!/bin/bash

size="30K_uni"

WA=""
#WA="-writesize"

#ARG=""
#ARG="-cmb"
ARG="-cmb -append"

[ -d "email" ] && rm email

echo "#############################################################" | tee email
echo "Breakdown I" | tee -a email

./merge_btree.out -new $WA $ARG -input ../../data/micro/micro_i_${size}.txt /dev/nvme0n1 
mv tree.dat micro_i_${size}_tree.dat
#./copy_tree.out dup_tree c
#cp dup_tree ./micro_i_${size}_tree

echo " Done " | tee -a email
echo "#############################################################" | tee -a email
#cat email | mail -s "Breakdown I Finish" josephly88@yahoo.com.hk
#rm email

echo "#############################################################" | tee email
echo "Breakdown U" | tee -a email

./merge_btree.out $WA $ARG -input ../../data/micro/micro_u_${size}.txt /dev/nvme0n1 
mv tree.dat micro_u_${size}_tree.dat

echo " Done " | tee -a email
echo "#############################################################" | tee -a email

if [[ "$WA" -eq "" ]]
then
    echo "#############################################################" | tee email
    echo "Breakdown R" | tee -a email

    ./merge_btree.out $WA $ARG -input ../../data/micro/micro_r_${size}.txt /dev/nvme0n1 
    mv tree.dat micro_r_${size}_tree.dat

    echo " Done " | tee -a email
    echo "#############################################################" | tee -a email
fi

echo "#############################################################" | tee email
echo "Breakdown D" | tee -a email

./merge_btree.out $WA $ARG -input ../../data/micro/micro_d_${size}.txt /dev/nvme0n1 
mv tree.dat micro_d_${size}_tree.dat

echo " Done " | tee -a email
echo "#############################################################" | tee -a email
#cat email | mail -s "Breakdown D Finish" josephly88@yahoo.com.hk
rm email
