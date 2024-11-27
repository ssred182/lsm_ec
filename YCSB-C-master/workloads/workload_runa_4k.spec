# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#                        
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

keylength=24
fieldcount=1048576
fieldlength=4096
recordcount=25
operationcount= 2621440
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0

requestdistribution=zipfian

