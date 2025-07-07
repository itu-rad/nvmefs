# Nvmefs

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, nvmefs, allow you to leverage NVMe SSD device features and bypassing Kernel filesystem layers using IO Passthru. The goal of the extension is to provide faster elapsed query times for I/O intensive queries.


## Building

### Prerequisites

To be able to build DuckDB, a guide can be found at the [DuckDB build guide](https://duckdb.org/docs/stable/dev/building/overview). 

Additionally, our repository requires Python with version 3.13.2 to be able run our tests.

### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/nvmefs/nvmefs.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `nvmefs.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To use the extension, start the DuckDB shell with:

```sh
./build/release/duckdb
```

Before first use, you must configure the extension. Please refer to the **Configuration** section for details. As an example, you can create a configuration with the following SQL:

```sql
CREATE PERSISTENT SECRET nvmefs (
  TYPE NVMEFS,
  nvme_device_path '/dev/ng1n0',
  backend          'io_uring_cmd'
);
```

After executing the above statement, restart DuckDB to ensure the extension picks up the new configuration.  
To verify that the configuration is active, run:

```sql
CALL config_print();
```

> **Note:**  
> If you encounter an error indicating the device cannot be opened, try running the DuckDB executable with elevated privileges (e.g., using `sudo`).

The extension registers a new file system handler for file paths prefixed with `nvmefs://`. To store data on the NVMe device using this extension, attach a database as follows:

```sql
ATTACH DATABASE 'nvmefs://example.db' AS nvme (READ_WRITE);
USE nvme;
```

You can now execute SQL statements against the attached database, leveraging the nvmefs extension.

## Running the Tests

This repository provides two types of test runners: unit/integration tests and end-to-end tests.  
- The unit and integration tests are located in the `./tests/gtest` directory.
- The end-to-end tests are located in the `./tests/e2e` directory.

To run the unit and integration tests, execute:

```bash
make gtest
```

To run the end-to-end tests, execute:

```bash
make e2e-test
```

> **Note:**  
> The end-to-end tests are implemented in Python. The provided make target will automatically set up a Python environment and install all

## Development

Developing the extension requires the tools provided by the DuckDB team. To simplify setup, we have created a development container (dev container) that includes all the necessary tools for contributing to this extension.

### IDE Setup

#### CLion

DuckDB provides a guide for using CLion, created by JetBrains. If you prefer CLion, follow the official setup guide: [Setting up CLion](https://github.com/duckdb/extension-template?tab=readme-ov-file#setting-up-clion).

#### VS Code

For VS Code, we strongly recommend using the Development Container. This container automatically installs all required dependencies and extensions.

##### Configuring VS Code

After launching the development container, some additional settings must be configured for CMake to function correctly:

1. Open the Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P` on macOS).
2. Search for and select **Preferences: Open Workspace Settings**.
3. In the settings UI, locate `cmake.sourceDirectory`.
4. Click **Edit in settings.json** and set the value to:

   ```json
   "cmake.sourceDirectory": "${workspaceFolder}/duckdb"
   ```

5. (Optional) If you use **Ninja** as the generator, add the following setting:

   ```json
   "cmake.generator": "Ninja"
   ```

#### Example `.vscode/settings.json`

Your final `.vscode/settings.json` should look like this:

```json
{
    "cmake.sourceDirectory": "${workspaceFolder}/duckdb",
    "cmake.generator": "Ninja" // OPTIONAL
}
```

This configuration ensures proper integration with the CMake extension in VS Code, streamlining the development workflow.

## Configuration

To fully utilize the extension, you must specify your configuration, primarily consisting of device information. Follow these steps:

1. Open DuckDB.
2. Create a new secret named `nvmefs`(See the section **Available Backend** for available backends):

   ```sql
   CREATE PERSISTENT SECRET nvmefs (
     TYPE NVMEFS,
     nvme_device_path <path_to_nvme_device>,
     backend          <storage backend to use>
   );
   ```

3. Restart DuckDB.
4. Your configuration is now saved.

### Available backends

he following backends are available. As the extension depends on the xNVMe library, the list below mirrors its supported backends:

| Backend       | value       |  Asynchronous?  |
|---------------|-------------|-----------------|
| io_uring      | io_uring    | true            |
| io_uring_cmd  | io_uring    | true            |
| libaio        | libaio      | true            |
| io_ring       | io_ring     | true            |
| posix         | posix       | true            |
| iocp          | iocp        | true            |
| iocp_th       | iocp_th     | true            |
| emu           | emu         | true            |
| thrpool       | thrpool     | true            |
| nil           | nil         | true            |
| spdk          | spdk_async  | true            |
| spdk          | spdk_sync   | false           |
| nvme          | nvme        | false           |

For details on operating system compatibility for each backend, refer to the [xNVMe backend documentation](https://xnvme.io/backends/index.html). 
