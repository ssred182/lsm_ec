#!/bin/bash
./exp_base.sh 3 thread_num_8 ra 8 1
./exp_base.sh 3 thread_num_8 ra 8 2
./exp_base.sh 3 thread_num_8 ra 8 3

./exp_base.sh 3 thread_num_16 ra 16 1
./exp_base.sh 3 thread_num_16 ra 16 2
./exp_base.sh 3 thread_num_16 ra 16 3

./exp_base.sh 3 thread_num_32 ra 32 1
./exp_base.sh 3 thread_num_32 ra 32 2
./exp_base.sh 3 thread_num_32 ra 32 3

./exp_base.sh 3 thread_num_64 ra 64 1
./exp_base.sh 3 thread_num_64 ra 64 2
./exp_base.sh 3 thread_num_64 ra 64 3


./exp_base.sh 3 thread_num_8 rc 8 1
./exp_base.sh 3 thread_num_8 rc 8 2
./exp_base.sh 3 thread_num_8 rc 8 3

./exp_base.sh 3 thread_num_16 rc 16 1
./exp_base.sh 3 thread_num_16 rc 16 2
./exp_base.sh 3 thread_num_16 rc 16 3

./exp_base.sh 3 thread_num_32 rc 32 1
./exp_base.sh 3 thread_num_32 rc 32 2
./exp_base.sh 3 thread_num_32 rc 32 3

./exp_base.sh 3 thread_num_64 rc 64 1
./exp_base.sh 3 thread_num_64 rc 64 2
./exp_base.sh 3 thread_num_64 rc 64 3

./exp_base_pbr.sh 3 thread_num_8 ra 8 1

./exp_base_pbr.sh 3 thread_num_16 ra 16 1

./exp_base_pbr.sh 3 thread_num_32 ra 32 1

./exp_base_pbr.sh 3 thread_num_64 ra 64 1

 ./exp_base_pbr.sh 3 thread_num_8 rc 8 1

./exp_base_pbr.sh 3 thread_num_16 rc 16 1

./exp_base_pbr.sh 3 thread_num_32 rc 32 1

./exp_base_pbr.sh 3 thread_num_64 rc 64 1
