#!/bin/bash

# Install xnvme libraries 
sudo bash -i ./scripts/xnvme/install.sh

# Install dev dependencies
sudo apt update

# TODO: Remove libboost-all-dev and use the libraries installed via vcpkg
sudo apt install -y ccache vim curl zip libboost-all-dev

# Setup tools directory
mkdir -p ~/.tools/bin
cd ~/.tools

git clone https://github.com/microsoft/vcpkg.git
cd vcpkg && ./bootstrap-vcpkg.sh

VCPKG_ROOT=~/.tools/vcpkg

echo "export VCPKG_ROOT='$VCPKG_ROOT'" >> ~/.bashrc
echo "export VCPKG_TOOLCHAIN_PATH='$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake'" >> ~/.bashrc
echo "export PATH='$PATH:$VCPKG_ROOT'" >> ~/.bashrc 

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.3/install.sh | bash

source ~/.bashrc

nvm install --lts
nvm use --lts

npm install -g editorconfig