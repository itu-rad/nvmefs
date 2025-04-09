#include <gtest/gtest.h>
#include "nvmefs.hpp"
#include "nvmefs_config.hpp"
#include "utils/gtest_utils.hpp"
#include "utils/fake_device.hpp"

namespace duckdb {

class NoDiskInteractionTest : public testing::Test {
protected:
	NoDiskInteractionTest() {
		// Set up the test environment
		file_system = make_uniq<NvmeFileSystem>(gtestutils::TEST_CONFIG, make_uniq<FakeDevice>(0));
	}

	unique_ptr<NvmeFileSystem> file_system;
};

class DiskInteractionTest : public testing::Test {

protected:
	DiskInteractionTest() {
		// Set up the test environment
		idx_t block_size = 4096;
		idx_t page_size = 4096 * 64;
		idx_t lba_count = (1ULL << 30) / block_size; // 1 GiB

		NvmeConfig testConfig {
		    .device_path = "/dev/ng1n1",
		    .plhdls = 8,
		    .max_temp_size = page_size * 10, // 10 pages = 640 blocks
		    .max_wal_size = 1ULL << 25       // 32 MiB
		};

		file_system = make_uniq<NvmeFileSystem>(testConfig, make_uniq<FakeDevice>(lba_count)); // 1 GiB
	}

	uint64_t end_of_db_lba;
	unique_ptr<NvmeFileSystem> file_system;
};

TEST_F(NoDiskInteractionTest, GetNameReturnsName) {
	string result = file_system->GetName();

	EXPECT_EQ(result, "NvmeFileSystem");
}

TEST_F(NoDiskInteractionTest, CanHandleFileValidPathReturnsTrue) {
	bool result = file_system->CanHandleFile("nvmefs://test.db");
	EXPECT_TRUE(result);
}

TEST_F(NoDiskInteractionTest, CanHandleFileInvalidPathReturnsFalse) {
	bool result = file_system->CanHandleFile("test.db");
	EXPECT_FALSE(result);
}

///// With disk interactions

TEST_F(DiskInteractionTest, FileSyncDoesNothingAsExpected) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	file_system->FileSync(*fh);
}

TEST_F(DiskInteractionTest, OnDiskFileReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_TRUE(file_system->OnDiskFile(*fh));
}

TEST_F(DiskInteractionTest, FileExistsNoMetadataReturnFalse) {
	bool result = file_system->FileExists("nvmefs://test.db");
	EXPECT_FALSE(result);
}

TEST_F(DiskInteractionTest, FileExistsConfirmsDatabaseExists) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Ensure that there is data in the database
	vector<char> hello_buf {'H', 'E', 'L', 'L', 'O'};
	fh->Write(hello_buf.data(), hello_buf.size());

	bool exists = file_system->FileExists("nvmefs://test.db");
	EXPECT_TRUE(exists);
}

TEST_F(DiskInteractionTest, FileExistGivenValidWALFileReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	vector<char> hello_buf {'H', 'E', 'L', 'L', 'O'};
	fh->Write(hello_buf.data(), hello_buf.size());

	bool exists = file_system->FileExists("nvmefs://test.db.wal");
	EXPECT_TRUE(exists);
}

TEST_F(DiskInteractionTest, FileExistsThrowsIOExceptionIfMultipleDatabases) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_THROW(file_system->FileExists("nvmefs://xyz.db"), IOException);
}

TEST_F(DiskInteractionTest, FileExistsReturnFalseWhenTemporaryFileDoNotExists) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	bool exists = file_system->FileExists("nvmefs:///tmp/file");
	EXPECT_FALSE(exists);
}

TEST_F(DiskInteractionTest, FileExistsReturnTrueWhenTemporaryFileExists) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write something to create file
	string hello = "hello temp";
	vector<char> hello_buf {hello.begin(), hello.end()};
	int bytes_to_read_write = hello.size();
	fh = file_system->OpenFile("nvmefs:///tmp/file", flags);
	fh->Write(hello_buf.data(), bytes_to_read_write);

	// Check if it exists
	bool exists = file_system->FileExists("nvmefs:///tmp/file");
	EXPECT_TRUE(exists);

	// Read back data
	vector<char> buffer(bytes_to_read_write);
	fh->Read(buffer.data(), bytes_to_read_write, 0);

	EXPECT_EQ(string(buffer.begin(), buffer.end()), hello);
}

TEST_F(DiskInteractionTest, GetFileSizeDbWithThreeBlocksReturnsThreeBlocksInBytes) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// Read and Write three blocks
	vector<char> buf_h {'H', 'E', 'L', 'L', 'O'};
	vector<char> buf_w {'W', 'O', 'R', 'L', 'D'};
	vector<char> buf_s {'S', 'M', 'I', 'L', 'E'};

	fh->Write(buf_h.data(), buf_h.size(), geo.lba_size * 0);
	fh->Write(buf_w.data(), buf_w.size(), geo.lba_size * 1);
	fh->Write(buf_s.data(), buf_s.size(), geo.lba_size * 2);

	vector<char> res_h(buf_h.size());
	vector<char> res_w(buf_w.size());
	vector<char> res_s(buf_s.size());

	fh->Read(res_h.data(), buf_h.size(), geo.lba_size * 0);
	fh->Read(res_w.data(), buf_w.size(), geo.lba_size * 1);
	fh->Read(res_s.data(), buf_s.size(), geo.lba_size * 2);

	EXPECT_EQ(res_h, buf_h);
	EXPECT_EQ(res_w, buf_w);
	EXPECT_EQ(res_s, buf_s);

	// Ensure correct file size of database
	int64_t expected = geo.lba_size * 3;
	int64_t result = file_system->GetFileSize(*fh);

	EXPECT_EQ(result, expected);
}

TEST_F(DiskInteractionTest, GetFileSizeWALWithNoEntriesReturnZeroBytes) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	fh = file_system->OpenFile("nvmefs://test.db.wal", flags);

	EXPECT_EQ(file_system->GetFileSize(*fh), 0);
}

TEST_F(DiskInteractionTest, GetFileSizeOpensTwoTemporaryFileReturnCorrectSizes) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	DeviceGeometry geo = file_system->GetDevice().GetDeviceGeometry();

	// Open two temporary files, write to the second
	unique_ptr<FileHandle> tmp_fh_1 = file_system->OpenFile("nvmefs:///tmp/file1", flags);
	unique_ptr<FileHandle> tmp_fh_2 = file_system->OpenFile("nvmefs:///tmp/file2", flags);

	vector<char> buf_h {'H', 'E', 'L', 'L', 'O'};
	tmp_fh_2->Write(buf_h.data(), buf_h.size(), geo.lba_size * 0);
	vector<char> res_h(buf_h.size());
	tmp_fh_2->Read(res_h.data(), buf_h.size(), geo.lba_size * 0);

	EXPECT_EQ(res_h, buf_h);

	//Check file sizes
	EXPECT_EQ(file_system->GetFileSize(*tmp_fh_1), 0);
	EXPECT_EQ(file_system->GetFileSize(*tmp_fh_2), geo.lba_size * 1);
}

TEST_F(DiskInteractionTest, DirectoryExistsNoLoadedMetadataReturnsFalse) {
	EXPECT_FALSE(file_system->DirectoryExists("nvmefs:///tmp"));
}

TEST_F(DiskInteractionTest, DirectoryExistsMetadataLoadedReturnsTrue) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_TRUE(file_system->DirectoryExists("nvmefs:///tmp"));
}

TEST_F(DiskInteractionTest, RemoveDirectoryGivenTemporyDirectoyRemovesSuccessfully) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write a file to temporary folder
	string temp_filename = "nvmefs:///tmp/file";
	vector<char> buf {'H', 'E', 'L', 'L', 'O'};
	fh = file_system->OpenFile(temp_filename, flags);
	fh->Write(buf.data(), buf.size());

	// Verify that it exists
	EXPECT_TRUE(file_system->FileExists(temp_filename));

	// Remove tmp directory and confirm deletion
	EXPECT_NO_THROW(file_system->RemoveDirectory("nvmefs:///tmp"));
	EXPECT_FALSE(file_system->FileExists(temp_filename));
}

TEST_F(DiskInteractionTest, RemoveDirectoryGivenInvalidDirectoryThrowsIOException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_THROW(file_system->RemoveDirectory("nvmefs://test.db/mydirectory"), IOException);
}

TEST_F(DiskInteractionTest, CreateDirectoryThrowsExpectionIfMetadataNotLoaded) {
	EXPECT_THROW(file_system->CreateDirectory("nvmefs:///tmp"), IOException);
}

TEST_F(DiskInteractionTest, CreateDirectoryNoExceptionThrownWhenMetadataLoaded) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_NO_THROW(file_system->CreateDirectory("nvmefs:///tmp"));
}


TEST_F(DiskInteractionTest, RemoveFileGivenWALRemovesWALData) {
	/*
	* We are going to write two blocks to the WAL, so:
	* | block1 = Hello | block2 = World | block3 = ? | ... blocks
	* Next write will target block3, however we delete WAL. Hence
	* next write will instead target block1
	*/
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write to the blocks
	string wal_filename = "nvmefs://test.db.wal";
	vector<char> buf_hello {'H', 'E', 'L', 'L', 'O'};
	vector<char> buf_world {'W', 'O', 'R', 'L', 'D'};
	fh = file_system->OpenFile(wal_filename, flags);
	fh->Write(buf_hello.data(), buf_hello.size(), 0);
	fh->Write(buf_world.data(), buf_world.size(), 4096); // 4096 = 1 block

	// Read back and confirm
	vector<char> res_hello(buf_hello.size());
	vector<char> res_world(buf_world.size());
	fh->Read(res_hello.data(), buf_hello.size(), 0);
	fh->Read(res_world.data(), buf_world.size(), 4096);
	EXPECT_EQ(res_hello, buf_hello);
	EXPECT_EQ(res_world, buf_world);

	// Remove WAL, write "fresh" and confirm
	EXPECT_NO_THROW(file_system->RemoveFile(wal_filename));
	vector<char> buf_fresh {'F', 'R', 'E', 'S', 'H'};
	vector<char> res_fresh(buf_fresh.size());
	fh->Write(buf_fresh.data(), buf_fresh.size());
	fh->Read(res_fresh.data(), buf_fresh.size(), 0);

	EXPECT_EQ(res_fresh, buf_fresh);
}

TEST_F(DiskInteractionTest, RemoveFileGivenValidTempFileRemovesIt) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	// Write temporary file
	string temp_filename = "nvmefs:///tmp/file";
	vector<char> buf {'H', 'E', 'L', 'L', 'O'};
	fh = file_system->OpenFile(temp_filename, flags);
	fh->Write(buf.data(), buf.size());

	// Verify that it exists
	EXPECT_TRUE(file_system->FileExists(temp_filename));

	// Remove and verify deletion
	file_system->RemoveFile(temp_filename);
	EXPECT_FALSE(file_system->FileExists(temp_filename));
}

TEST_F(DiskInteractionTest, RemoveFileGivenPathBesidesTMPOrWALDoesNothing) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);

	EXPECT_NO_THROW(file_system->RemoveFile("nvmefs://test.db"));
}

TEST_F(DiskInteractionTest, OpenFileCompleteInvalidPathThrowInvalidInputException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_THROW(file_system->OpenFile("nvmefs://test", flags), InvalidInputException);
}

TEST_F(DiskInteractionTest, OpenFileInvalidDBPathThrowIOException) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_THROW(file_system->OpenFile("nvmefs://test.wal", flags), IOException);
}

TEST_F(DiskInteractionTest, OpenFileValidDBPathThrowNoExpeception) {
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	ASSERT_NO_THROW(file_system->OpenFile("nvmefs://test.db", flags));
}

TEST_F(DiskInteractionTest, OpenFileProducesCorrectFileHandle){
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = file_system->OpenFile("nvmefs://test.db", flags);
	NvmeFileHandle& nvme_fh = fh->Cast<NvmeFileHandle>();
	NvmeFileSystem& nvme_fs = nvme_fh.file_system.Cast<NvmeFileSystem>();


	EXPECT_EQ(nvme_fh.path, "nvmefs://test.db");
	EXPECT_EQ(nvme_fh.flags.OpenForWriting(), true);

	// Check that underlying filesytem is the same
	EXPECT_EQ(&nvme_fs, &*file_system);
}

TEST_F(DiskInteractionTest, WriteAndReadData) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	int data_size = strlen(data_ptr);
	file->Write(data_ptr, data_size, 0);

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 0);

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), data_ptr);
}

TEST_F(DiskInteractionTest, WriteAndReadDataDoesNotOverlapOtherCategories) {

	// Create a file
	string file_path = "nvmefs://test.db";
	string wal_file_path = "nvmefs://test.db.wal";
	string tmp_file_path =
	    StringUtil::Format("nvmefs://test.db/tmp/duckdb_temp_storage_%d-%llu.tmp", DEFAULT_BLOCK_ALLOC_SIZE, 0);
	unique_ptr<FileHandle> db_file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(db_file != nullptr);

	unique_ptr<FileHandle> wal_file =
	    file_system->OpenFile(wal_file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(wal_file != nullptr);

	unique_ptr<FileHandle> tmp_file =
	    file_system->OpenFile(tmp_file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(tmp_file != nullptr);

	// Write some data to the db file
	char *db_data_ptr = "Hello, db!";
	int db_data_size = strlen(db_data_ptr);
	db_file->Write(db_data_ptr, db_data_size, 0);

	char *wal_data_ptr = "Hello, wal!";
	int wal_data_size = strlen(wal_data_ptr);
	wal_file->Write(wal_data_ptr, wal_data_size, 0);

	char *tmp_data_ptr = "Hello, tmp!";
	int tmp_data_size = strlen(tmp_data_ptr);
	tmp_file->Write(tmp_data_ptr, tmp_data_size, 0);

	// Read the data back
	vector<char> db_buffer(db_data_size);
	db_file->Read(db_buffer.data(), db_data_size, 0);

	vector<char> wal_buffer(wal_data_size);
	wal_file->Read(wal_buffer.data(), wal_data_size, 0);

	vector<char> tmp_buffer(tmp_data_size);
	tmp_file->Read(tmp_buffer.data(), tmp_data_size, 0);

	// Check that the data is correct
	EXPECT_EQ(string(db_buffer.data(), db_data_size), db_data_ptr);
	EXPECT_EQ(string(wal_buffer.data(), wal_data_size), wal_data_ptr);
	EXPECT_EQ(string(tmp_buffer.data(), tmp_data_size), tmp_data_ptr);
}

// TODO: Make this parameterized to test different byte offsets whithin different blocks
TEST_F(DiskInteractionTest, WriteAndReadDataWithinBlock) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	int data_size = strlen(data_ptr);
	file->Write(data_ptr, data_size, 16); // Write data at the 16th byte of the device

	// Read the data back
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 16); // Read data from the 16th byte of the device

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), data_ptr);
}

TEST_F(DiskInteractionTest, WriteAndReadDataWithSeek) {

	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	idx_t block_location = 4096 * 5; // 5 blocks of 4096 bytes each

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	int data_size = strlen(data_ptr);
	file->Write(data_ptr, data_size, block_location);

	// Seek to the beginning of the file
	file->Seek(4096 * 3);

	// The old 5th block should now be translated to the 0th block after seek
	vector<char> buffer(data_size);
	file->Read(buffer.data(), data_size, 4096 * 2);

	// Check that the data is correct
	EXPECT_EQ(string(buffer.data(), data_size), data_ptr);
}

TEST_F(DiskInteractionTest, SeekOutOfBounds) {
	// Create a file
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Attempt to seek out of bounds
	EXPECT_THROW(file->Seek((1ULL << 31) + 1), std::runtime_error);
}

TEST_F(DiskInteractionTest, ReadAndWriteReturningNumberOfBytes) {
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	uint64_t bytes_written = file->Write(data_ptr, 13);

	// Read the data
	vector<char> buffer(13);
	uint64_t bytes_read = file->Read(buffer.data(), 13);

	// Check that the number of bytes written and read is correct
	EXPECT_EQ(bytes_written, 13);
	EXPECT_EQ(bytes_read, 13);
	EXPECT_EQ(string(buffer.data(), bytes_read), data_ptr);
}

TEST_F(DiskInteractionTest, ReadWithReturnIOfBytesAfterSettingSeek) {
	string file_path = "nvmefs://test.db";
	unique_ptr<FileHandle> file =
	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	ASSERT_TRUE(file != nullptr);

	// Write some data to the file
	char *data_ptr = "Hello, World!";
	file->Write(data_ptr, 13, 4096 * 64);

	// Move file pointer to next duckdb page
	file->Seek(4096 * 64);

	// Read the data
	vector<char> buffer(13);
	uint64_t bytes_read = file->Read(buffer.data(), 13);

	// Check that the number of bytes written and read is correct
	EXPECT_EQ(bytes_read, 13);
	EXPECT_EQ(string(buffer.data(), bytes_read), data_ptr);
}

// TODO: Think about how this should be handled when the file system defines the ranges of LBA's
// TEST_F(DiskInteractionTest, WriteOutOfRange) {
// 	// Create a file
// 	string file_path = "nvmefs://test.db";
// 	unique_ptr<FileHandle> file =
// 	    file_system->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
// 	ASSERT_TRUE(file != nullptr);

// 	// Attempt to write data out of range
// 	char *data_ptr = "Hello, World!";
// 	int data_size = strlen(data_ptr);
// 	EXPECT_THROW(file->Write(data_ptr, data_size, (1ULL << 30) + 1), std::runtime_error);
// }

} // namespace duckdb
