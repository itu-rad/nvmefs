#!/bin/bash

NUM_BLOCKS=$(nvme id-ctrl /dev/nvme1n1 | grep 'tnvmcap' | sed 's/,//g' | awk -v BS=4096 '{print $3/BS}')
nvme dsm /dev/nvme1n1 --namespace-id=1 --ad -s 0 -b $NUM_BLOCKS