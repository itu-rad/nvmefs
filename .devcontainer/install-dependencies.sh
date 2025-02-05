#!/bin/bash

# Install xnvme libraries 
bash -i ./scripts/xnvme/install.sh

# Install dev dependencies
apk update
apk add --upgrade libcurl
apk --no-cache add cmake ccache vim curl zip

# Setup tools directory
mkdir -p /root/.tools/bin
cd /root/.tools

git clone https://github.com/microsoft/vcpkg.git
cd vcpkg && ./bootstrap-vcpkg.sh

VCPKG_ROOT=~/.tools/vcpkg

echo "export VCPKG_ROOT='$VCPKG_ROOT'" >> ~/.bashrc
echo "export PATH='$PATH:$VCPKG_ROOT'" >> ~/.bashrc 