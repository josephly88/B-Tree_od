#!/bin/bash

num="_5M"   # MODIFT
SSD_PATH="/dev/nvme0n60340736"  # MODIFT

data_path="../../data/"
wl_name="micro"                                     #CHECK
wl_path=$data_path$wl_name
mode=("LeafCache" "Both")      #CHECK
workload=("micro_i" "micro_r" "micro_u" "micro_d")  #CHECK
cp_path="checkpoint/"

[ -d "email" ] && rm email
[ -d "*.dat" ] && rm *.dat

for m in "${mode[@]}"; do
    for w in ${workload[@]}; do
        ../../proc_data/cmb_counter -r
        sleep 1

        echo "#############################################################" | tee email
        echo " Dataset Size is $num " | tee -a email
        echo " Current mode is $m " | tee -a email
        echo " Current workload is $w " | tee -a email

        # Load the inserted state
        #cp Loaded/$m/micro_i${num}_cmb dup_cmb
        #cp Loaded/$m/micro_i${num}_tree dup_tree
        #./copy_cmb.out dup_cmb p        
        #sleep 1
        #./copy_tree.out dup_tree p        
        #sleep 1

        arg=""
        if [ $m = "CMB" ]
        then
            arg="-cmb"
        elif [ $m = "Append" ]
        then
            arg="-cmb -append"
        elif [ $m = "LeafCache" ]
        then
            arg="-cmb -lfcache"
        elif [ $m = "Both" ]
        then
            arg="-cmb -append -lfcache"
        fi

        if [ $w = "micro_i" ]
        then
            ./merge_btree.out -new $arg -input $wl_path/$w$num.txt $SSD_PATH
        else
            ./merge_btree.out $arg -input $wl_path/$w$num.txt $SSD_PATH
        fi

        [ ! -d ${cp_path}${wl_name} ] && mkdir ${cp_path}${wl_name}
        [ ! -d ${cp_path}${wl_name}/${m} ] && mkdir ${cp_path}${wl_name}/${m}
    
        CP=${cp_path}${wl_name}/${m}

        mv ${w}${num}.dat $CP/${w}${num}.dat
        mv tree.dat $CP/${w}${num}_tree.dat
        echo " Copy tree " 
        ./copy_tree.out dup_tree c
        cp dup_tree $CP/${w}${num}_tree

        if [ "$m" = "CMB" ] || [ "$m" = "Append" ] || [ "$m" = "LeafCache" ] || [ "$m" = "Both" ]
        then
            echo " Copy CMB " 
            ./copy_cmb.out dup_cmb c
            cp dup_cmb $CP/${w}${num}_cmb
        fi
        
        echo " Done " | tee -a email
        echo $'#############################################################' | tee -a email
        ../../proc_data/cmb_counter >> email 
        cat email | mail -s "Workload Finish" josephly88@yahoo.com.hk
        rm email
    done
done
