#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

"""
Production-Grade High-Performance Database to RocksDB Migration Engine.

This script features:
1. Multi-Source Support: Auto-detects and migrates from either PostgreSQL or SQLite.
2. Dynamic Schema Auto-Discovery: Queries DB metadata (no hardcoded schemas).
3. Parallel Multi-threaded extraction & insertion pools.
4. Intelligent JSON Column handling (resolving double-serialization issues).
5. Memory-efficient chunked streaming.
6. High-throughput atomic write batches for RocksDB.
"""

import argparse
import concurrent.futures
import datetime
import json
import sqlite3
import sys
import threading
from decimal import Decimal
from typing import Any, Generator

import psycopg2
from psycopg2.extras import DictCursor

from synapse.storage.engines.rocksdb_schema import get_rocksdb_key

try:
    import rocksdict

    HAS_ROCKSDICT = True
except ImportError:
    HAS_ROCKSDICT = False

# Thread-local storage for database connections
thread_local = threading.local()


class CustomEncoder(json.JSONEncoder):
    """
    Handles converting Postgres/SQLite-specific types cleanly into JSON bytes.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        return super().default(obj)


def get_source_connection(pg_dsn: str | None, sqlite_path: str | None) -> Any:
    """
    Gets or creates a thread-local database connection for either Postgres or SQLite.
    """
    if pg_dsn:
        if not hasattr(thread_local, "pg_conn"):
            thread_local.pg_conn = psycopg2.connect(pg_dsn)
        return thread_local.pg_conn
    elif sqlite_path:
        if not hasattr(thread_local, "sqlite_conn"):
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            thread_local.sqlite_conn = conn
        return thread_local.sqlite_conn
    else:
        raise ValueError("No database source provided.")


def discover_table_schema(
    conn: Any, table_name: str, is_postgres: bool
) -> tuple[list[str], tuple[str, ...]]:
    """
    Queries database metadata catalogs to auto-discover table columns and primary keys.
    Supports both PostgreSQL catalogs and SQLite PRAGMAs.
    """
    if is_postgres:
        with conn.cursor() as cur:
            # 1. Fetch columns in table definition order
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            columns = [row[0] for row in cur.fetchall()]

            if not columns:
                raise ValueError(
                    f"Postgres table '{table_name}' does not exist or has no columns."
                )

            # 2. Fetch primary key column names
            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
                """,
                (table_name,),
            )
            pks = tuple(row[0] for row in cur.fetchall())

            # Fallback to the first column if no explicit PK
            if not pks:
                pks = (columns[0],)

            return columns, pks
    else:
        # SQLite
        with conn.cursor() as cur:
            cur.execute(f"PRAGMA table_info({table_name})")
            rows = cur.fetchall()
            if not rows:
                raise ValueError(
                    f"SQLite table '{table_name}' does not exist or has no columns."
                )

            columns = []
            pks_list = []
            for row in rows:
                # SQLite Row returns mapping or tuple.
                # SQLite PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
                col_name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
                is_pk = row["pk"] if isinstance(row, sqlite3.Row) else row[5]
                columns.append(col_name)
                if is_pk:
                    pks_list.append(col_name)

            pks = tuple(pks_list) if pks_list else (columns[0],)
            return columns, pks


def chunked_database_reader(
    conn: Any,
    table_name: str,
    columns: list[str],
    is_postgres: bool,
    batch_size: int = 20000,
) -> Generator[list[dict[str, Any]], None, None]:
    """
    Streams rows from Postgres/SQLite in memory-efficient chunks.
    Uses Postgres server-side cursors, or standard SQLite batch queries.
    """
    columns_str = ", ".join(f'"{col}"' for col in columns)

    if is_postgres:
        cursor_name = f"srv_cursor_{table_name}_{threading.get_ident()}"
        with conn.cursor(name=cursor_name, cursor_factory=DictCursor) as srv_cursor:
            srv_cursor.itersize = batch_size
            srv_cursor.execute(f"SELECT {columns_str} FROM {table_name}")
            while True:
                rows = srv_cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(row) for row in rows]
    else:
        # SQLite cursors are naturally light, we stream row by row with fetchmany
        with conn.cursor() as cur:
            cur.execute(f"SELECT {columns_str} FROM {table_name}")
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                # Convert SQLite.Row to Dict
                yield [dict(row) for row in rows]


def clean_json_field(val: Any) -> Any:
    """
    Decodes pre-serialized string JSONs (like Synapse's event contents or custom JSONs)
    to prevent double JSON serialization in RocksDB value payloads.
    """
    if isinstance(val, str) and (val.startswith(("{", "["))):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            pass
    return val


def migrate_single_table_worker(
    pg_dsn: str | None,
    sqlite_path: str | None,
    rocksdb_path: str,
    table_name: str,
    batch_size: int,
    use_fallback: bool,
    fallback_lock: threading.Lock,
    fallback_store: dict[str, bytes],
) -> int:
    """
    Worker function executed in parallel to migrate a single database table.
    """
    is_postgres = pg_dsn is not None
    conn = get_source_connection(pg_dsn, sqlite_path)

    try:
        columns, pks = discover_table_schema(conn, table_name, is_postgres)
    except Exception as e:
        print(f"[-] Thread for '{table_name}' failed schema discovery: {e}")
        return 0

    print(f"[*] Thread started for table '{table_name}' (PKs: {pks})")

    # Thread-Safe RocksDB connection
    db = None
    if HAS_ROCKSDICT and not use_fallback:
        db = rocksdict.Rdict(rocksdb_path)

    total_migrated = 0
    batch_index = 0

    try:
        for row_batch in chunked_database_reader(
            conn, table_name, columns, is_postgres, batch_size
        ):
            batch_index += 1

            if db:
                write_batch = rocksdict.WriteBatch()
            else:
                write_batch = None

            local_batch_items = []

            for row in row_batch:
                pk_vals = tuple(row.get(pk) for pk in pks)
                if any(val is None for val in pk_vals):
                    continue

                cleaned_row = {k: clean_json_field(v) for k, v in row.items()}

                key = get_rocksdb_key(table_name, pk_vals)
                value = json.dumps(cleaned_row, cls=CustomEncoder).encode("utf-8")

                if write_batch:
                    write_batch[key] = value
                else:
                    local_batch_items.append((key.decode("utf-8"), value))

            # Commit to storage
            if write_batch and db:
                db.write(write_batch)
            elif use_fallback:
                with fallback_lock:
                    for k, v in local_batch_items:
                        fallback_store[k] = v

            total_migrated += len(row_batch)

        print(f"[+] Successfully migrated table '{table_name}': {total_migrated} rows.")
        return total_migrated

    except Exception as e:
        print(f"[-] Error migrating table '{table_name}': {e}")
        return total_migrated
    finally:
        if db:
            db.close()


def discover_all_tables(pg_dsn: str | None, sqlite_path: str | None) -> list[str]:
    """
    Discovers all non-system tables present in the source database.
    """
    if pg_dsn:
        conn = psycopg2.connect(pg_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                      AND table_name NOT LIKE 'pg_%%'
                      AND table_name NOT LIKE 'sql_%%'
                    """
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
    elif sqlite_path:
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name NOT LIKE 'sqlite_%%'
                    """
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
    else:
        raise ValueError("No database source provided.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parallel Auto-Discovered Postgres/SQLite to RocksDB Migration Engine."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pg-dsn", help="Postgres connection DSN")
    group.add_argument("--sqlite-path", help="Path to SQLite database file")

    parser.add_argument(
        "--rocksdb-path",
        default="rocksdb_experimental_store",
        help="Path to RocksDB database directory",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20000,
        help="Batch size for ETL streaming and commits",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of tables to migrate concurrently",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to migrate (default: auto-discover all tables)",
    )

    args = parser.parse_args()

    # 1. Discover all tables in the source database
    try:
        all_tables = discover_all_tables(args.pg_dsn, args.sqlite_path)
        source_type = "PostgreSQL" if args.pg_dsn else "SQLite"
        print(
            f"[+] Connected to {source_type}. Auto-discovered {len(all_tables)} tables."
        )
    except Exception as e:
        print(f"[-] Failed to connect or fetch table catalog: {e}")
        sys.exit(1)

    target_tables = args.tables if args.tables else all_tables
    print(f"[*] Target tables for migration: {len(target_tables)}")

    # 2. Manage Fallback store locks
    use_fallback = not HAS_ROCKSDICT
    fallback_lock = threading.Lock()
    fallback_store: dict[str, bytes] = {}

    if use_fallback:
        print(
            "[!] rocksdict not found. Running in thread-safe fallback JSON dictionary mode."
        )

    # 3. Concurrent thread pool executor
    print(f"[*] Starting parallel executor pool (Concurrency={args.concurrency})...")
    total_rows_migrated = 0

    # If SQLite, restrict concurrency to 1 to avoid file lock issues in sqlite3 multi-threaded reading
    actual_concurrency = 1 if args.sqlite_path else args.concurrency
    if args.sqlite_path and args.concurrency > 1:
        print(
            "[!] SQLite source detected. Restricting concurrency to 1 to prevent database locking."
        )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=actual_concurrency
    ) as executor:
        futures = {
            executor.submit(
                migrate_single_table_worker,
                args.pg_dsn,
                args.sqlite_path,
                args.rocksdb_path,
                table,
                args.batch_size,
                use_fallback,
                fallback_lock,
                fallback_store,
            ): table
            for table in target_tables
        }

        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                rows = future.result()
                total_rows_migrated += rows
            except Exception as e:
                print(f"[-] Migration of table '{table}' raised an exception: {e}")

    # 4. Save fallback file if running fallback mode
    if use_fallback:
        fallback_file = f"{args.rocksdb_path}.json"
        print(
            f"[*] Saving {len(fallback_store)} rows to fallback file: {fallback_file}..."
        )
        try:
            decoded_fallback = {k: v.decode("utf-8") for k, v in fallback_store.items()}
            with open(fallback_file, "w") as f:
                json.dump(decoded_fallback, f, indent=2)
            print("[+] Successfully saved fallback store.")
        except Exception as e:
            print(f"[-] Error saving fallback JSON file: {e}")

    print(f"\n[+] MIGRATION COMPLETED! Total rows migrated: {total_rows_migrated}")


if __name__ == "__main__":
    main()
