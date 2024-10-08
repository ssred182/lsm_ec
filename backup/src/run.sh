clear
rm -f run.txt
rm -r ../rocksdb_lsm_0
rm -r ../rocksdb_lsm_1
rm -r ../rocksdb_lsm_2
rm -r ../rocksdb_lsm_3
rm -r ../parity_sgement
mkdir ../parity_sgement
g++ -std=c++17 -o test main.cpp backup.cpp encode.cpp value_sgement.cpp rdma_connect.cpp ~/rocksdb/librocksdb.a -lisal -libverbs -lpthread -lsnappy  -lz -lbz2 -lzstd /usr/lib/x86_64-linux-gnu/liblz4.a
./test -> run.txt