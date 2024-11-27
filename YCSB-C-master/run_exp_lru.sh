#!/bin/bash

./exp_ec.sh 3 thread_num_16 rc 16 1

./exp_ec.sh 3 thread_num_32 rc 32 1

./exp_ec.sh 3 thread_num_64 rc 64 1

./exp_lru.sh 3 thread_num_16 rc 16 1

./exp_lru.sh 3 thread_num_32 rc 32 1

./exp_lru.sh 3 thread_num_64 rc 64 1