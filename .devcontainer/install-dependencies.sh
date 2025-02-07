#!/bin/bash

# Install xnvme libraries 
sudo bash -i ./scripts/xnvme/install.sh

# Install dev dependencies
sudo apt update
sudo apt install -y ccache vim curl zip

# Setup tools directory
mkdir -p ~/.tools/bin
cd ~/.tools

git clone https://github.com/microsoft/vcpkg.git
cd vcpkg && ./bootstrap-vcpkg.sh

VCPKG_ROOT=~/.tools/vcpkg

echo "export VCPKG_ROOT='$VCPKG_ROOT'" >> ~/.bashrc
echo "export PATH='$PATH:$VCPKG_ROOT'" >> ~/.bashrc 

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.3/install.sh | bash

source ~/.bashrc

nvm install --lts
nvm use --lts

npm install -g editorconfig