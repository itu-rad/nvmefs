# Nvmefs

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Nvmefs, allow you to ... <extension_goal>.


## Building
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
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `nvmefs()` that takes a string arguments and returns a string:
```
D select nvmefs('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Nvmefs Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL nvmefs
LOAD nvmefs
```

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
