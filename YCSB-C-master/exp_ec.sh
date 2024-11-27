#!/bin/bash
exp_round=$1
exp_lable=$2
load=$3
thread_num=$4
round=$5
source setting.sh


exp_name=exp${exp_round}_${exp_lable}_${load}_${round}_ec
./command.sh 2 2 ${exp_name}
./ycsbc -db mydb -threads ${thread_num} -lru 0 -P workloads/workload_${load}.spec -p0 ${primary0} -p1 ${primary1} -p2 ${primary2} -p3 ${primary3} -b0 ${backup0} -b1 ${backup1} -e0 ${extra0} -e1 ${extra1} -mode slimKV -> ${exp_name}_thp.txt
sleep 5
bash kill_all.sh
