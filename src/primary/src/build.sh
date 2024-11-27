rm -f *.txt
rm ../rocksdb_lsm/*
rm ../value_sgement/*
g++ -w -std=c++17 -o primary main.cpp db.cpp value_sgement.cpp rdma_connect.cpp librocksdb.a -libverbs -lpthread
rm ../test/primary
cp primary ../test
