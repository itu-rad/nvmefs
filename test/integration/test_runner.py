import argparse
from dataclasses import dataclass
import os
import duckdb
import pytest

from utils.device import NvmeDevice

@dataclass
class Arguments:
    extension_dir_path: str = "../../build/release/extension/nvmefs"

    def valid(self) -> bool:
        
        return True

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "-e",
            "--extension_dir_path",
            type=str,
            help="Path to the directory where the extension is located",
            default="../../build/release/extension")

        args = parser.parse_args()
        
        arguments: Arguments = Arguments(
            args.extension_dir_path
        )

        if not arguments.valid():
            parser.print_help()
            exit(1)
        
        return arguments

if __name__ == "__main__":
    args = Arguments.parse_args()
    duckdb.sql(f"INSTALL nvmefs FROM '{args.extension_dir_path}';") # Ensures that when we call "LOAD nvmefs" that it can be found in the extension directory

    pytest.main(["--device_path", "hello world"])

    # args = Arguments.parse_args()
    # extension_filepath = os.path.join(args.extension_dir_path, "nvmefs.duckdb_extension")
    # con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

    # con.sql("PRAGMA platform;").show()
    # con.load_extension(extension_filepath)
    # con.sql("from duckdb_extensions()").show()

    # duckdb.connect("nvmefs:///test.db")