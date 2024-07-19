#!/bin/bash

size="2_5M"

ARG="-cmb -append"
APP_NUM=(4 8 16 32)
#ARG="-cmb"
#APP_NUM=("")

[ -d "email" ] && rm email

for AP in "${APP_NUM[@]}"
do
    name="AP${AP}"

    echo "#############################################################" | tee email
    echo "Breakdown I ${name}" | tee -a email

    ./merge_btree.out -new $ARG $AP -input ../../data/app_test/insert_${size}.txt /dev/nvme0n1 
    mv insert_${size}.dat ${name}_insert_${size}.dat
    mv tree.dat ${name}_insert_${size}_tree.dat

    echo " Done " | tee -a email
    echo "#############################################################" | tee -a email
    cat email | mail -s "Breakdown I ${name} Finish" josephly88@yahoo.com.hk
    rm email

    echo "#############################################################" | tee email
    echo "Breakdown U ${name}" | tee -a email

    ./merge_btree.out $ARG $AP -input ../../data/app_test/update_${size}.txt /dev/nvme0n1 
    mv update_${size}.dat ${name}_update_${size}.dat
    mv tree.dat ${name}_update_${size}_tree.dat

    echo " Done " | tee -a email
    echo "#############################################################" | tee -a email
    cat email | mail -s "Breakdown U ${name} Finish" josephly88@yahoo.com.hk
    rm email

    echo "#############################################################" | tee email
    echo "Breakdown R ${name}" | tee -a email

    ./merge_btree.out $ARG $AP -input ../../data/app_test/search_${size}.txt /dev/nvme0n1 
    mv search_${size}.dat ${name}_search_${size}.dat
    mv tree.dat ${name}_search_${size}_tree.dat

    echo " Done " | tee -a email
    echo "#############################################################" | tee -a email
    cat email | mail -s "Breakdown R ${name} Finish" josephly88@yahoo.com.hk
    rm email

    echo "#############################################################" | tee email
    echo "Breakdown D ${name}" | tee -a email

    ./merge_btree.out $ARG $AP -input ../../data/app_test/delete_${size}.txt /dev/nvme0n1 
    mv delete_${size}.dat ${name}_delete_${size}.dat
    mv tree.dat ${name}_delete_${size}_tree.dat

    echo " Done " | tee -a email
    echo "#############################################################" | tee -a email
    cat email | mail -s "Breakdown D ${name} Finish" josephly88@yahoo.com.hk
    rm email
done
