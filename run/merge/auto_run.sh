#!/bin/bash

num="_5M"   # MODIFT
SSD_PATH="/dev/nvme0n60340736"  # MODIFT

#mode=("" "-cmb" "-dram")
mode=("-cmb")
#workload=("insert" "read_and_update" "delete")
workload=("insert" "read_and_update")

for m in "${mode[@]}"; do
    for w in ${workload[@]}; do
        echo " Current mode is $m "
        echo " Current workload is $w "

        if [ $w = "insert" ]
        then
            ./merge_btree.out -new $m -input ../../data/$w$num.txt $SSD_PATH
        else
            ./merge_btree.out $m -input ../../data/$w$num.txt $SSD_PATH
        fi
        mv ${w}${num}.dat checkpoint/${w}${num}${m}.dat
        mv tree.dat checkpoint/${w}${num}${m}_tree.dat
        echo " Copy tree " 
        ./copy_tree.out dup_tree c
        cp dup_tree checkpoint/${w}${num}${m}_tree

        if [ "$m" = "-cmb" ]
        then
            echo " Copy CMB " 
            ./copy_cmb.out dup_cmb c
            cp dup_cmb checkpoint/${w}${num}${m}_cmb
        fi

        if [ "$m" = "-cmb" ] || [ "$m" = "-dram" ]
        then
            cp dup_cmb checkpoint/${w}${num}${m}_cmb
        fi
        
        cp node_access.txt checkpoint/node_access-${w}.txt
        
        chmod 444 checkpoint/*
    done
done
