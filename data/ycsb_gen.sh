#!/bin/bash

num="_1k"

bin/ycsb run basic -P workloads/workloada -P ../property.dat > ../insert$num.txt
bin/ycsb run basic -P workloads/workloada -P ../property.dat > ../A$num.txt
bin/ycsb run basic -P workloads/workloadb -P ../property.dat > ../B$num.txt
bin/ycsb run basic -P workloads/workloadc -P ../property.dat > ../C$num.txt
bin/ycsb run basic -P workloads/workloadd -P ../property.dat > ../D$num.txt
bin/ycsb run basic -P workloads/workloadf -P ../property.dat > ../F$num.txt
