#include "gtest_utils.hpp"

namespace duckdb {
	namespace gtestutils {
		static bool DeallocDevice() {
			string dealloc_device_cmd = "sh ../../../scripts/nvme/device_dealloc.sh";

			int err = system(dealloc_device_cmd.c_str());
			if (err) {
				return false;
			}
			return true;
		}
	}
}
