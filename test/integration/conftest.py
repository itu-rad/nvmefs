import pytest

from utils.device import NvmeDevice

def pytest_addoption(parser):

    parser.addoption(
        "--device_path",
        type=str,
        help="Path to the device to be used for the extension",
        default="/dev/ng1n1"
    )


@pytest.fixture(scope="session")
def device_path(pytestconfig):
    return pytestconfig.getoption("device_path")

@pytest.fixture(scope="module")
def device(device_path):
    device = NvmeDevice(device_path)

    yield device

    device.deallocate(1)