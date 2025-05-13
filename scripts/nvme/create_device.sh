NUM_BLOCKS=$(nvme id-ctrl /dev/nvme1 | grep 'tnvmcap' | sed 's/,//g' | awk -v BS=4096 '{print $3/BS}')
nvme create-ns /dev/nvme1 -b 4096 --nsze=$NUM_BLOCKS --ncap=$NUM_BLOCKS
nvme attach-ns /dev/nvme1 --namespace-id=1 --controllers=0x7