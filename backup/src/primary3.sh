clear
rm ../rocksdb_lsm/*
rm ../value_segment/*
./primary -c 10.118.0.40 -C 10.118.0.68 -d mlx5_0 -i 1 -g 3 -n 3 -m 10.118.0.53 -e 10.118.0.15 -E 10.118.0.16