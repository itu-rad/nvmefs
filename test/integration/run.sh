#!/bin/bash

EXTENSION_DIR=$1
CURRENT_DIR=$(pwd)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "/home/pinar/.bashrc"
source "${SCRIPT_DIR}/init.sh"

cd "${SCRIPT_DIR}"
xnvme-driver
pytest --extension_dir_path="../../build/release/extension/nvmefs" --device="0000\:ec\:00" --spdk --pci="0000:ec:00"
xnvme-driver reset
cd "${CURRENT_DIR}"