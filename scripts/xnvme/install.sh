#!/bin/bash

############################################
######## Install xNVMe Dependencies ########
############################################
# Description:
#   This script is responsible for building and installing
#   xNVMe libraries to your machine, using the files from
#   the attached git submodule 'third-party/xnvme'.
#
# Assumptions:
#   It is assumed that this script is being executed from
#   the root of the nvmefs project directory. 
#   Additionally, the files in the submodule has to be
#   available and fetched. If not, search for how 
#   to fetch a git submodules files.

# OLD_PWD="${PWD}"
# XNVME_DEV_ROOT="./third-party/xnvme"

# Change working directory
cd $XNVME_DEV_ROOT

# configure xNVMe and build meson subprojects(SPDK)
meson setup builddir

# build xNVMe
meson compile -C builddir

# install xNVMe
meson install -C builddir