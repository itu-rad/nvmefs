#!/bin/bash

############################################
########### Setup CI Toolchains ############
############################################
# Description:
#  This script is used to setup the custom toolchain
#  for the nvmefs extension. This script is called
#  by the CI/CD pipeline (via "make configure_ci") 
#  to setup the custom toolchain
#
# Assumptions:
#   It is assumed that this script is being executed from
#   the root of the nvmefs project directory. 
#   Additionally, the files in the submodule has to be
#   available and fetched. If not, search for how 
#   to fetch a git submodules files.

# note that the $DUCKDB_PLATFORM environment variable can be used to discern between the platforms
echo "This is the sample custom toolchain script running for architecture '$DUCKDB_PLATFORM' for the nvmefs extension."

if ["${DUCKDB_PLATFORM}" == "linux_amd64_musl"]; then
    apt-get install -y ninja-build
fi

python -m pip install --user meson
export PATH=$PATH:/Users/runner/Library/Python/3.11/bin

meson --version
ninja --version

bash ./scripts/xnvme/install.sh
