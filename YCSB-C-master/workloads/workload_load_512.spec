# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#                        
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

keylength=24
fieldcount=1048576
fieldlength=512
recordcount=200
operationcount= 20971520
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

requestdistribution=zipfian

