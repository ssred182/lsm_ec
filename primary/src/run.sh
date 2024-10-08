rm -f run.txt
rm -r ../rocksdb_lsm
rm -r ../rocksdb_lsm2
rm -r ../value_sgement
mkdir ../value_sgement
g++ -std=c++17 -o rocksdbtest test.cpp db.cpp value_sgement.cpp ~/rocksdb/librocksdb.a -lpthread -lsnappy  -lz -lbz2 -lzstd /usr/lib/x86_64-linux-gnu/liblz4.a
./rocksdbtest >> run.txt