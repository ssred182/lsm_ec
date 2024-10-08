clear
rm ../rocksdb_lsm/*
rm ../value_sgement/*
./primary -b 1 -c 10.118.0.53 -C 10.118.0.53 -d mlx5_1 -i 1 -g 3 -n 0 -m 10.118.0.53 -e 10.118.0.53 -E 10.118.0.53
