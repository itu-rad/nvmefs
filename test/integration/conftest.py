import pytest
import duckdb
import os

from utils.device import NvmeDevice

def pytest_addoption(parser):

    parser.addoption(
        "--extension_dir_path",
        type=str,
        help="Path to the extension directory",
        default="../../build/release/extension/nvmefs"
    )

    parser.addoption(
        "--device",
        type=str,
        help="Path to the device to be used for the extension",
        default="/dev/ng1n1"
    )

    parser.addoption(
        "--spdk",
        help="Use spdk",
        action="store_true",
        default=False
    )

    parser.addoption(
        "--pci",
        type=str,
        help="PCI address if spdk is used"
    )

@pytest.fixture(scope="session")
def configure(pytestconfig):
    path = pytestconfig.getoption("extension_dir_path")
    duckdb.sql(f"INSTALL nvmefs FROM '{path}';") # Ensures that when we call "LOAD nvmefs" that it can be found in the extension directory

    return True

@pytest.fixture(scope="session")
def device_path(configure, pytestconfig):
    if configure is False:
        pytest.fail("Extension not configured properly") 

    return pytestconfig.getoption("device")



@pytest.fixture(scope="module")
def device(device_path, pytestconfig):
    device = NvmeDevice(device_path)

    spdk = pytestconfig.getoption("spdk")

    if spdk:
        pci_address = pytestconfig.getoption("pci")
        device.device_path = pci_address
        os.system("HUGEMEM=4096 xnvme-driver")

    yield device

    if spdk:
        device.device_path = device_path
        os.system("xnvme-driver reset")

    device.deallocate(1)