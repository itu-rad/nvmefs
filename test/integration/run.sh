#!/bin/bash

EXTENSION_DIR=$1

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "${SCRIPT_DIR}/init.sh"

python3 "${SCRIPT_DIR}/test_runner.py" --extension_dir_path $EXTENSION_DIR --device "/dev/ng1n1"