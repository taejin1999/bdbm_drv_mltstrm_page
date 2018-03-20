cd ~/Research/Multi-Stream/benchmarks/rocksdb-master/experiment
if [ $1 -eq 1 ] 
then
./run_db_bench.sh
elif [ $1 -eq 2 ]
then
./run_db_bench_append.sh
elif [ $1 -eq 3 ] 
then
./run_db_bench_fill.sh
fi

cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
