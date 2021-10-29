# NVMFS

This project stores file metadata in NVM-DB and stores filedata in DAOS.

This project include two FUSE-based filesystems：Metafs and Rocksfs. Metafs must work on NVM and Rocksfs needn't.

# Source Code Structure
- MetaDB: A non-volatile memory-based high performance KV storage developed by my senior. I use it to store filesystem's metadata in metafs.

- metafs：A filesystem using MetaDB to store filesystem's metadata.

- rocksfs: A filesystem using RocksDB to store filesystem's metadata.

> Use `cmake` and `make` to compile Metafs or Rocksfs.

> Use `test.sh` in `xxfs`'s folder to mount and use the filesystem.

> Use filebench to test performance.
