#!/bin/bash  
BACKUP_MODE_P=$1
BACKUP_MODE_B=$2
#USE_LRU=$3
exp_name=$3
source setting.sh

echo "primary0"
ssh -p ${port_p0} wangxu@${primary0} " cd ~/slimKV/primary0/test ; rm ../rocksdb_lsm/* ;
rm ../value_sgement/* ;
./primary -n 0 -b ${BACKUP_MODE_P} -c ${backup0} -C ${backup1} -d mlx5_1 -i 1 -g 3 -m ${master} -e ${extra0} -E ${extra1} -> ${exp_name}.txt " &

echo "primary1"
ssh -p ${port_p1} wangxu@${primary1} " cd ~/slimKV/primary1/test ; rm ../rocksdb_lsm/* ;
rm ../value_sgement/* ;
./primary -n 1 -b ${BACKUP_MODE_P} -c ${backup0} -C ${backup1} -d mlx5_1 -i 1 -g 3 -m ${master} -e ${extra0} -E ${extra1} -> ${exp_name}.txt " &

echo "primary2"
ssh -p ${port_p2} wangxu@${primary2} " cd ~/slimKV/primary2/test ; rm ../rocksdb_lsm/* ;
rm ../value_sgement/* ;
./primary -n 2 -b ${BACKUP_MODE_P} -c ${backup0} -C ${backup1} -d mlx5_1 -i 1 -g 3 -m ${master} -e ${extra0} -E ${extra1} -> ${exp_name}.txt " &

echo "primary3"
ssh -p ${port_p3} wangxu@${primary3} " cd ~/slimKV/primary3/test ; rm ../rocksdb_lsm/* ;
rm ../value_sgement/* ;
./primary -n 3 -b ${BACKUP_MODE_P} -c ${backup0} -C ${backup1} -d mlx5_1 -i 1 -g 3 -m ${master} -e ${extra0} -E ${extra1} -> ${exp_name}.txt " &

echo "backup0"
ssh -p ${port_b0} wangxu@${backup0} "cd ~/slimKV/backup0/test ; rm ../parity_sgement/*
rm ../value_sgement/* ; rm ../rocksdb_lsm_0/* ; rm ../rocksdb_lsm_1/* ; rm ../rocksdb_lsm_2/* ; rm ../rocksdb_lsm_3/* ;
./backup -b ${BACKUP_MODE_B} -d mlx5_0 -i 1 -g 3 -n 0 -s -m ${master} -e ${extra0} -E ${extra1} -> ${exp_name}.txt " &

echo "backup1"
ssh -p ${port_b1} wangxu@${backup1} "cd ~/slimKV/backup1/test ; rm ../parity_sgement/*
rm ../value_sgement/* ; rm ../rocksdb_lsm_0/* ; rm ../rocksdb_lsm_1/* ; rm ../rocksdb_lsm_2/* ; rm ../rocksdb_lsm_3/* ;
./backup -b ${BACKUP_MODE_B} -d mlx5_0 -i 1 -g 3 -n 1 -s -m ${master} -e ${extra0} -E ${extra1} -> ${exp_name}.txt " &

echo "extra0"
ssh -p ${port_e0} wangxu@${extra0} "cd ~/slimKV/extra0/test ; rm ../parity_sgement/*
rm ../value_sgement/* ; rm ../rocksdb_lsm_0/* ; rm ../rocksdb_lsm_1/* ; rm ../rocksdb_lsm_2/* ; rm ../rocksdb_lsm_3/* ;
rm ../rocksdb_lsm/* ;
./extra -d mlx5_0 -i 1 -g 3 -n 0 -c ${backup0} -C ${backup1} -m ${master} -> ${exp_name}.txt " & 

echo "extra1"
ssh -p ${port_e1} wangxu@${extra1} "cd ~/slimKV/extra1/test ; rm ../parity_sgement/*
rm ../value_sgement/* ; rm ../rocksdb_lsm_0/* ; rm ../rocksdb_lsm_1/* ; rm ../rocksdb_lsm_2/* ; rm ../rocksdb_lsm_3/* ;
rm ../rocksdb_lsm/* ;
./extra -d mlx5_0 -i 1 -g 3 -n 1 -c ${backup0} -C ${backup1} -m ${master} -> ${exp_name}.txt " & 

echo "master"
ssh -p ${port_m} wangxu@${master} "cd ~/slimKV/master/test ; 
./master -d mlx5_0 -i 1 -g 3 -> ${exp_name}.txt " &


 

