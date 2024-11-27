rm ../../extra0/rocksdb_lsm/*
rm ../../extra0/rocksdb_lsm_0/*
rm ../../extra0/rocksdb_lsm_1/*
rm ../../extra0/rocksdb_lsm_2/*
rm ../../extra0/rocksdb_lsm_3/*
rm ../../extra0/value_sgement/*
rm ../../extra1/rocksdb_lsm/*
rm ../../extra1/value_sgement/*
g++ -w -std=c++17 -o extra extra.cpp value_sgement.cpp backup.cpp encode.cpp db.cpp rdma_connect.cpp librocksdb.a libisal.a -libverbs -lpthread -ltbb
#-lisal -lsnappy  -lz -lbz2 -lzstd /usr/lib/x86_64-linux-gnu/liblz4.a
cp extra ../test
