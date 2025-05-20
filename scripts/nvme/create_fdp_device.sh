#!/bin/bash

NUM_BLOCKS=$(nvme id-ctrl /dev/nvme1 | grep 'tnvmcap' | sed 's/,//g' | awk -v BS=4096 '{print $3/BS}')
nvme set-feature /dev/nvme1 -f 0x1D -c 1 -s
nvme create-ns /dev/nvme1 -b 4096 --nsze=$NUM_BLOCKS --ncap=$NUM_BLOCKS --nphndls=7 --phndls=0,1,2,3,4,5,6
nvme attach-ns /dev/nvme1 --namespace-id=1 --controllers=0x7