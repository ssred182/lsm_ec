rm ../rocksdb_lsm_0/*
rm ../rocksdb_lsm_1/*
rm ../rocksdb_lsm_2/*
rm ../rocksdb_lsm_3/*
rm ../parity_sgement/*
rm ../value_sgement/*
./backup -d mlx5_0 -i 1 -g 3 -n 0 -s -m 10.118.0.52 -e 10.118.0.52 -E 10.118.0.52
