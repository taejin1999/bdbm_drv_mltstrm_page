sudo rm /tmp/lifetime*
sudo rm /tmp/gc*

#sudo insmod ../../devices/ramdrive/risa_dev_ramdrive.ko
sudo insmod ../../devices/ramdrive_timing/risa_dev_ramdrive_timing.ko
sudo insmod robusta_drv.ko _param_tech_type=$1 _param_display_num=$2
sudo mkfs -t ext4 -b 4096 /dev/robusta
sudo mount -t ext4 -o discard /dev/robusta /mnt/blueDBM

#tail -F /var/log/kern.log
