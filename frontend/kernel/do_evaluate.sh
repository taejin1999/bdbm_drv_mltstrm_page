#!/bin/bash
repeat=$1

bench=$2



tech=0
cur=0
workload=0

for ((cur=0; cur<repeat; cur++))
do
	for ((tech=0; tech<9; tech++))
	do
		#echo ${display_margin_array[$index]}
		if [ $tech -eq 0 -o $tech -eq 2 -o $tech -eq 4 -o $tech -eq 5 -o $tech -eq 6 ]
		then
		./eval.sh $tech $bench 
		fi
	done
done
cd ~/Research/Multi-Stream/benchmarks/rocksdb-master/experiment/backup
dt=`date +%m%d%H%M`
sudo cp /tmp/waf.log ./waf$dt.log
sudo chown tjkim:tjkim ./waf$dt.log
sudo rm /tmp/waf.log
cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
