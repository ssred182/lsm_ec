#!/bin/bash
source setting.sh


scp -r -P ${port_p0} ../src/primary ${user}@${primary0}:~/slimKV/
ssh -p ${port_p0} ${user}@${primary0} " cd ~/slimKV ; mv primary primary0 ; cd primary0/src ; make ; cp primary ../test/ "
scp -r -P ${port_p1} ../src/primary ${user}@${primary1}:~/slimKV/
ssh -p ${port_p0} ${user}@${primary0} " cd ~/slimKV ; mv primary primary1 ; cd primary1/src ; make ; cp primary ../test/ "
scp -r -P ${port_p2} ../src/primary ${user}@${primary2}:~/slimKV/
ssh -p ${port_p0} ${user}@${primary0} " cd ~/slimKV ; mv primary primary2 ; cd primary2/src ; make ; cp primary ../test/ "
scp -r -P ${port_p3} ../src/primary ${user}@${primary3}:~/slimKV/
ssh -p ${port_p0} ${user}@${primary0} " cd ~/slimKV ; mv primary primary3 ; cd primary3/src ; make ; cp primary ../test/ "

scp -r -P ${port_b0} ../src/backup ${user}@${backup0}:~/slimKV/
ssh -p ${port_b0} ${user}@${backup0} " cd ~/slimKV ; mv backup backup0 ; cd backup0/src ; make b ; cp backup ../test/"
scp -r -P ${port_b1} ../src/backup ${user}@${backup1}:~/slimKV/
ssh -p ${port_b1} ${user}@${backup1} " cd ~/slimKV ; mv backup backup1 ; cd backup1/src ; make b ; cp backup ../test/"

ssh -P ${port_e0} ${user}@${extra0} " cd ~/SlimKV ;
mkdir extra0;
cd extra0;
mkdir test;
mkdir rocksdb_lsm ;
mkdir rocksdb_lsm_0 ;
mkdir rocksdb_lsm_1 ;
mkdir rocksdb_lsm_2 ;
mkdir rocksdb_lsm_3 ;
mkdir value_sgement  "
scp -r -P ${port_e0} ../src/backup/src ${user}@${extra0}:~/slimKV/extra0/
ssh -P ${port_e0} ${user}@${extra0} "cd ~/SlimKV/extra0/src ; make e ; cp extra ../test/"
ssh -P ${port_e1} ${user}@${extra1} " cd ~/SlimKV ;
mkdir extra1;
cd extra1;
mkdir test;
mkdir rocksdb_lsm ;
mkdir rocksdb_lsm_0 ;
mkdir rocksdb_lsm_1 ;
mkdir rocksdb_lsm_2 ;
mkdir rocksdb_lsm_3 ;
mkdir value_sgement  "
scp -r -P ${port_e1} ../src/backup/src ${user}@${extra1}:~/slimKV/extra1/
ssh -P ${port_e1} ${user}@${extra1} "cd ~/SlimKV/extra1/src ; make e ; cp extra ../test/"

ssh -P ${port_m} ${user}@${master} " cd ~/SlimKV ; mkdir master ; cd master; mkdir test"
scp -r -P ${port_m} ../src/backup/src ${user}@${master}:~/slimKV/master/
ssh -P ${port_m} ${user}@${master} " cd ~/SlimKV/master/src ; make m ; cp master ../test/"
