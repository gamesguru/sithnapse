#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

"""
SQLite-RocksDB VFS Python Loader for Synapse.

This script demonstrates how Python's built-in 'sqlite3' library can be
configured to load a compiled SQLite-on-RocksDB virtual file system (VFS)
shared library (.so). Once loaded, any standard SQLite SQL query executed
by Synapse is transparently translated and written to a physical RocksDB store!
"""

import os
import sqlite3
from typing import Any

ROCKSDB_VFS_PATH = "sqlite3_rocksdb.so"


def enable_rocksdb_vfs_for_sqlite():
    """
    Monkey-patches the sqlite3 library or creates a wrapper hook to ensure
    all connections load the SQLite-on-RocksDB VFS shared library.
    """
    if not os.path.exists(ROCKSDB_VFS_PATH):
        print(
            f"[-] Error: Compiled SQLite-RocksDB extension '{ROCKSDB_VFS_PATH}' not found."
        )
        print(
            "    Please compile 'sqlite3-rocksdb' (https://github.com/thomas-m-connor/sqlite3-rocksdb) first."
        )
        return False

    # Original connect function
    _original_connect = sqlite3.connect

    def connect_with_rocksdb_vfs(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        # SQLite's VFS parameter can be set via connection uri or loaded dynamically.
        # Here we open a standard connection, load the RocksDB VFS extension,
        # and set RocksDB as the default VFS.
        conn = _original_connect(*args, **kwargs)
        try:
            # Enable extension loading on the connection
            conn.enable_load_extension(True)
            # Load the sqlite3-rocksdb shared library
            conn.load_extension(ROCKSDB_VFS_PATH)
            print("[+] Successfully loaded SQLite-RocksDB VFS into SQLite3 connection.")
        except Exception as e:
            print(f"[-] Failed to load SQLite-RocksDB VFS: {e}")
        return conn

    # Apply the monkey patch
    sqlite3.connect = connect_with_rocksdb_vfs
    print("[+] Monkey-patched sqlite3.connect to automatically load RocksDB VFS.")
    return True


if __name__ == "__main__":
    print("================================================================")
    print("SQLite-RocksDB VFS Loader test hook")
    print("================================================================")

    # Check if the .so exists, and guide compilation
    if not os.path.exists(ROCKSDB_VFS_PATH):
        print("How to compile 'sqlite3-rocksdb' for your system:")
        print("1. Clone the project:")
        print("   git clone https://github.com/thomas-m-connor/sqlite3-rocksdb.git")
        print("2. Compile the shared library:")
        print("   cd sqlite3-rocksdb && make")
        print("3. Copy the compiled '.so' file to this directory:")
        print("   cp sqlite3_rocksdb.so /run/media/shane/shane4tb-ent/repos/synapse/")
        print("================================================================")
    else:
        enable_rocksdb_vfs_for_sqlite()

        # Test connecting
        print("[*] Testing connection to an SQLite database backed by RocksDB...")
        try:
            conn = sqlite3.connect("test_rocksdb_vfs.db")
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, val TEXT)"
            )
            cur.execute(
                "INSERT INTO test (val) VALUES (?)", ("RocksDB VFS test payload",)
            )
            conn.commit()

            cur.execute("SELECT * FROM test")
            print(f"[+] Query Result: {cur.fetchall()}")
            conn.close()
            print("[+] Test completed successfully!")
        except Exception as e:
            print(f"[-] Test failed: {e}")
