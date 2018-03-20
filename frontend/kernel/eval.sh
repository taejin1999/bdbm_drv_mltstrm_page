#!/bin/bash

display_margin_array=(21000000 16000000 2000000)
index=$2-1
dsp=${display_margin_array[$index]}
echo Mount!
cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
./mount.sh $1 $dsp
cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
if [ $1 -eq 4 -o $1 -eq 5 ]
then 
	cd /usr/src/linux-3.16.43/pc/
	./insmod.sh
	cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
fi
echo Run DB!
./run_db.sh $2
sleep 1
cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
echo Unmount!
./umount.sh
echo Backup!
./run_backup.sh $1
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
if [ $1 -eq 4 -o $1 -eq 5 ]
then 
	cd /usr/src/linux-3.16.43/pc/
	./rmmod.sh
	cd /home/tjkim/Research/bdbm_drv_mltstrm_page/frontend/kernel
fi



