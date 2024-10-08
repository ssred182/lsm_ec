rm -r ../../node0/rocksdb_lsm_0
rm -r ../../node0/rocksdb_lsm_1
rm -r ../../node0/rocksdb_lsm_2
rm -r ../../node0/rocksdb_lsm_3
rm -r ../../node0/parity_sgement
mkdir ../../node0/parity_sgement
rm -r ../../node0/value_sgement
mkdir ../../node0/value_sgement
rm -r ../../node1/rocksdb_lsm_0
rm -r ../../node1/rocksdb_lsm_1
rm -r ../../node1/rocksdb_lsm_2
rm -r ../../node1/rocksdb_lsm_3
rm -r ../../node1/parity_sgement
mkdir ../../node1/parity_sgement
rm -r ../../node1/value_sgement
mkdir ../../node1/value_sgement
clear
g++ -std=c++17 -o test test.cpp backup.cpp encode.cpp value_sgement.cpp rdma_connect.cpp ~/rocksdb/librocksdb.a -lisal -libverbs -lpthread -lsnappy  -lz -lbz2 -lzstd /usr/lib/x86_64-linux-gnu/liblz4.a
cp test ../../node0/test
cp test ../../node1/test