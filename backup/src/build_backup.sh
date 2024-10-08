rm -f run.txt
rm ../rocksdb_lsm_0/*
rm ../rocksdb_lsm_1/*
rm ../rocksdb_lsm_2/*
rm ../rocksdb_lsm_3/*
rm ../parity_sgement/*
rm ../value_sgement/*
clear
g++ -w -std=c++17 -o backup main.cpp backup.cpp encode.cpp value_sgement.cpp rdma_connect.cpp libisal.a librocksdb.a  -libverbs -lpthread 
#-lsnappy  -lz -lbz2 -lzstd /usr/lib/x86_64-linux-gnu/liblz4.a -lisal
rm ../test/backup
cp backup ../test