#!/bin/bash

# Install xnvme libraries 
bash -i ./scripts/xnvme/install.sh

# Install dev dependencies
apk --no-cache add cmake ccache
