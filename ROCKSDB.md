# Experimental RocksDB Storage Backend for Matrix Synapse

This document outlines the architecture, setup, migration, and configuration
instructions for the experimental RocksDB storage backend developed on the
`guru/feat/rocksdb-migration` branch.

---

## 1. Prerequisites

To use the native RocksDB store, you must install the official C++ RocksDB
Python bindings (`rocksdict`) in your virtual environment:

```bash
# Install dependencies including rocksdict
poetry install
```

---

## 2. Converting an Existing PostgreSQL or SQLite Database to RocksDB

We have developed a high-performance, dynamic, and multi-threaded ETL migration
engine that streams your live relational database and converts it into a
binary RocksDB key-value store.

### Playbook

1. **Stop Synapse** to freeze the database state and prevent data drift:

   ```bash
   synctl stop
   ```

2. **Execute the Migration Script**:
   Set `--pg-dsn` (for PostgreSQL) OR `--sqlite-path` (for SQLite) to point to
   your active source database:

   ```bash
   # Migrate from PostgreSQL to RocksDB
   poetry run python scripts-dev/migrate_postgres_to_rocksdb.py \
       --pg-dsn "dbname=synapse user=postgres password=secret host=localhost" \
       --rocksdb-path "rocksdb_experimental_store" \
       --concurrency 4

   # OR Migrate from SQLite to RocksDB
   poetry run python scripts-dev/migrate_postgres_to_rocksdb.py \
       --sqlite-path "homeserver.db" \
       --rocksdb-path "rocksdb_experimental_store"
   ```

---

## 3. Configuring Synapse to use RocksDB

To tell Synapse to run natively on your new RocksDB database instead of
PostgreSQL or SQLite, modify your **`homeserver.yaml`** configuration file.

### Before (PostgreSQL Example)

```yaml
database:
  name: psycopg2
  args:
    user: postgres
    password: secret_password
    database: synapse
    host: localhost
```

### After (RocksDB Setup)

Replace your entire database block with the following:

```yaml
database:
  name: rocksdb
  args:
    db_path: "rocksdb_experimental_store"
```

Then start Synapse back up:

```bash
synctl start
```

---

## 4. Alternative Approaches with SQL-on-RocksDB Engines

Because migrating all of Synapse's complex SQL queries (JOINs, recursive CTEs)
to pure key-value pairs is an ongoing architectural shift, you can leverage
two existing wrappers to run SQL natively on top of RocksDB with **zero code
modifications**.

We have pre-packaged automated setups for both of these approaches in this
branch.

### Option A: YugabyteDB (PostgreSQL-on-RocksDB)

YugabyteDB uses PostgreSQL's query planner but writes all data directly into a
highly optimized C++ RocksDB-derived storage engine (DocDB).

- **How to run natively on Linux (No Docker)**

  ```bash
  # Download, install, and run YugabyteDB natively
  ./scripts-dev/setup_yugabyte_postgres.sh
  ```

- **Synapse Configuration**
  Point your `homeserver.yaml` to Yugabyte's Postgres port (`5433`).

---

### Option B: SQLite-RocksDB VFS

This approach loads a custom compiled Virtual File System (VFS) extension into
Python's `sqlite3` module, routing standard SQLite files into RocksDB.

- **How to run**

  ```bash
  # Test the load hook and view compilation instructions
  poetry run python scripts-dev/setup_sqlite_rocksdb.py
  ```

---

## 5. Architectural Implementation Details

For developers wishing to contribute to the native RocksDB backend:

- **Database Engine (`synapse/storage/engines/rocksdb.py`)**
  Conforms to the `BaseDatabaseEngine` abstract class, stubbing out traditional
  relational concepts while exposing a DBAPI2-compliant cursor and connection
  wrapper for RocksDB.

- **Schema Definition (`synapse/storage/engines/rocksdb_schema.py`)**
  Defines primary key structures and column ordering metadata to partition table
  spaces using key prefixes (e.g. `profiles:<user_id>`).

- **Native DAO Bypass (`synapse/storage/databases/main/profile.py`)**
  Demonstrates the native KV-read/write bypass pattern. For instance,
  `get_profileinfo()` intercepts requests when `rocksdb` is active to execute
  a high-speed `$key` lookup directly on RocksDB, completely bypassing the
  SQL query compiler.
