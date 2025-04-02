import argparse
from dataclasses import dataclass
import os
import duckdb
import pytest

from utils.device import NvmeDevice

@dataclass
class Arguments:
    extension_dir_path: str = "../../build/release/extension/nvmefs"
    device: str = None

    def valid(self) -> bool:

        if self.device is None:
            print("No device path provided")
            return False
        
        return True

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "-d",
            "--device",
            type=str,
            help="File path to the device to run the benchmark on(/dev/nvme1)",
            default=None
        )

        parser.add_argument(
            "-e",
            "--extension_dir_path",
            type=str,
            help="Path to the directory where the extension is located",
            default="../../build/release/extension")

        args = parser.parse_args()
        
        arguments: Arguments = Arguments(
            args.extension_dir_path,
            args.device
        )

        if not arguments.valid():
            parser.print_help()
            exit(1)
        
        return arguments

if __name__ == "__main__":
    args = Arguments.parse_args()
    duckdb.sql(f"INSTALL nvmefs FROM '{args.extension_dir_path}';") # Ensures that when we call "LOAD nvmefs" that it can be found in the extension directory

    # pytest_args = [f"--device", f"{args.device}"] # TODO: Fix this to be able to be passed to pytest
    pytest.main(args=[])