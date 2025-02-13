# FileSystem Overview

The filesystem of DuckDb is structured as a composite pattern. 

```mermaid
---
title: FileSystem Implementations
---
classDiagram
    class client
    class FileSystem {
        + RegisterSubSystem()
        + OpenFile()
        + Read()
        + Write()
        + CanHandleFile()
    }
    class VirtualFileSystem {
        - vector~unique_ptr~FileSystem~~ sub_system 
        - FileSystem default_fs
        - map~CompressionType, FileSystem~ compression_fs 
        - FindFileSystem() FileSystem
        - FindFileSystemInternal() FileSystem
    }
    client --> FileSystem
    VirtualFileSystem --|> FileSystem
    VirtualFileSystem o--> FileSystem
    LocalFileSystem --|> FileSystem
    GZipFileSystem --|> FileSystem
    PipeFileSystem --|> FileSystem
```

The `VirtualFileSystem` is composite class that contains the other implementations of `FileSystem`. By using `FindFileSystem()` it iterates through the vector of file systems and call `CanHandleFile()` until the first implementation of `FileSystem` returns `true`.  

DuckDB File system works like the following:

### OpenFile()

```mermaid
---
title: Sequence of operations when calling OpenFile() in DuckDB
---
sequenceDiagram
    DatabaseFileSystem ->>+ VirtualFileSystem: OpenFile(filepath, flags, file_opener)
    loop Every FileSystem registered
        VirtualFileSystem ->>+ FileSystem: CanHandleFile(filepath)
        alt FileSystem can handle the file
            FileSystem -->> VirtualFileSystem: return true
            VirtualFileSystem ->> VirtualFileSystem: break loop
        else FileSystem cannot handle the file
            FileSystem -->>- VirtualFileSystem: return false
        end
    end
    alt FileSystem not found
        VirtualFileSystem ->> VirtualFileSystem: Use default FileSystem
    end
    VirtualFileSystem ->>+ FileSystem: OpenFile()
    create participant FileHandler
    FileSystem ->> FileHandler: new FileHandler(file_system, filepath, flags, file_opener)
    FileSystem -->>- VirtualFileSystem: return FileHandler
    VirtualFileSystem -->>- DatabaseFileSystem: return FileHandler
```
#### List of Participants

- **DatabaseFileSystem**: Is responsible for wrapping the `VirtualFileSystem` and provide a context to the opening of files. For that reason the `DatabaseFileSystem` chain of implements is: `DatabaseFileSystem` --> `OpenerFileSystem` --> `FileSystem`. The context is of type `FileOpener` and provides the current settings configured in the database.

### Write()

When calling `Write()` there are different scenarios on how this is being performed. The below diagram shows how the temporary files are being written to using Write()([see here](https://github.com/duckdb/duckdb/blob/19864453f7d0ed095256d848b46e7b8630989bac/src/storage/temporary_file_manager.cpp#L123)). The file handler has already been created, using `OpenFile()` as shown above and is available in this scenario.

```mermaid
---
title: Sequence of operations when calling Write() from the TemporaryFileHandler
---
sequenceDiagram
    activate TemporaryFileHandler
    TemporaryFileHandler ->>+ FileBuffer: Write(file_handler, location)
    FileBuffer ->>+ FileHandler: Write(buffer, nr_bytes, location)
    FileHandler ->>+ FileSystem: Write(this, buffer, nr_bytes, location)
    FileSystem ->> FileSystem: Write data from buffer to file
    FileSystem -->>- FileHandler: return
    FileHandler -->>- FileBuffer: return
    FileBuffer -->>- TemporaryFileHandler: return

    deactivate TemporaryFileHandler
```

Another scenario is when write is being called from the VirtualFileSystem. In this case the `VirtualFileSystem` skips the call to `Write()`on the `FileHandler` but instead calls `Write()` on the `FileSystem`.

```mermaid
---
title: Sequence of operations when calling Write() from the VirtualFileSystem
---
sequenceDiagram
    actor DuckDB-subsystem
    DuckDB-subsystem ->>+ VirtualFileSystem: Write(file_handler, buffer, nr_bytes, location)
    VirtualFileSystem ->>+ FileSystem: Write(file_handler, buffer, nr_bytes, location)
    FileSystem ->> FileSystem: Write data from buffer to file
    FileSystem -->>- VirtualFileSystem: return
    VirtualFileSystem -->>- DuckDB-subsystem: return
```