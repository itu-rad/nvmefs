#!/bin/bash

EXTENSION_DIR=$1
CURRENT_DIR=$(pwd)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "/home/pinar/.bashrc"
source "${SCRIPT_DIR}/init.sh"

cd "${SCRIPT_DIR}"
pytest -s --extension_dir_path="../../build/release/extension/nvmefs" --device="/dev/nvme1n1" --spdk --pci="0000:ec:00"
cd "${CURRENT_DIR}"