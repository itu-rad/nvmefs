#!/bin/bash

echo 'export PATH=$PATH:/Users/runner/Library/Python/3.11/bin' >> ~/.bashrc 
source ~/.bashrc

echo $PATH

bash ./scripts/xnvme/install.sh