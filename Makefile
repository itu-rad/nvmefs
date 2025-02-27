PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=nvmefs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

run:
	@./build/release/duckdb

clean-run: clean release run

configure_ci:
	@bash "./scripts/setup-custom-toolchain.sh"
