# SlimKV
## Prepare machines
4 primary nodes
2 backup nodes
2 extra nodes
1 master nod
1 client node
make sure client node can ssh others without password
### setting
modify setting.sh in YCSB-C-master
## Project Structure
### The following folders contain
YCSB-C-master contains the C++ version of the YCSB benchmark along with a SlimKV client. We control our project in YCSB-C-master/src.
src/primary/src contains source code of primary nodes.
src/backup/src contains source code of backup/extra/master nodes.

## Build Dependencies
We build all dependencies machine of client first.
### Infiniband verbs

### RocksDB
We use RocksDB in v6.6.4.
We slightly change the source code of RocksDB for primary nodes.
Please use files in "rocksdb_change_file" to replace some files in RocksDB.
Copy static library of RocksDB("librocksdb.a") to ~/SlimKV/src/priarmy/src/ and ~/SlimKV/src/backup/src.
### ISA-L
Follow the instruction in https://github.com/intel/isa-l
Copy static library of libisal.a to ~/SlimKV/src/backup/src.
Add include file into INCLUDE-PATH
### oneTBB
Follow the instruction in https://github.com/oneapi-src/oneTBB/.
Install oneTBB in all backup/extra/client nodes.
## Build SlimKV
### Try compilation
#### primary
cd ~/SlimKV/src/primary
make
#### backup
cd ~/SlimKV/src/backup
make b
#### extra
make e
#### master
make m
#### client
cd ~/SlimKV/YCSB-C-master
make
### Build code
cd ~/SlimKV/src/primary
./build_dir.sh
cd ~/SlimKV/src/backup
./build_dir.sh
cd ~/SlimKV/YCSB-C-master
./build_all_code.sh

## Run
cd ~/SlimKV/YCSB-C-master
### script
exp_base.sh is for SlimKV
exp_base_pbr.sh is for PBR
exp_normal_gc.sh is for SlimKV without sync-gc
exp_ec.sh is for SlimKV without subread
exp_lru.sh is for SlimKV replace subread with LRU read cache
### config script
./exp_xx.sh ${exp_id} ${exp_label} ${workload} ${thread_nums} ${rounds}


