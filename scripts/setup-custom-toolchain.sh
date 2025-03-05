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


if [ "${DUCKDB_PLATFORM}" == "linux_amd64_musl" ] && [ "${LINUX_CI_IN_DOCKER}" == 0 ]; then
    sudo python -m pip install meson
    sudo apt-get install -y ninja-build
    sudo bash ./scripts/xnvme/ci-install.sh
else
    # python -m pip install --user meson
    # export PATH=$PATH:/Users/runner/Library/Python/3.11/bin
    # bash ./scripts/xnvme/install.sh
    apk add -qq python3
    python -m ensurepip --upgrade --user
    python -m pip install meson
    bash ./scripts/xnvme/ci-install.sh
fi
