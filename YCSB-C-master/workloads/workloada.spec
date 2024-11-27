# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

keylength=24
fieldcount=1048576
fieldlength=1000
recordcount=16
operationcount= 10485760
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=zipfian

