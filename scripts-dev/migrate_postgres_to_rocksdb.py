#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

"""
Production-grade Postgres to RocksDB Migration ETL Script.
This script extracts tables from PostgreSQL in memory-efficient batches,
maps them to the RocksDB key-value schema format, handles type conversions
(bytea, datetime, etc.), and writes them using high-speed atomic write batches.
"""

import argparse
import datetime
import json
import sys
from decimal import Decimal
from typing import Any, Generator

import psycopg2
from psycopg2.extras import DictCursor

from synapse.storage.engines.rocksdb_schema import (
    ROCKSDB_TABLE_SCHEMAS,
    get_rocksdb_key,
)

try:
    import rocksdict

    HAS_ROCKSDICT = True
except ImportError:
    HAS_ROCKSDICT = False


class CustomEncoder(json.JSONEncoder):
    """
    Custom JSON Encoder to handle non-standard types returned by PostgreSQL.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            # Encode binary data (bytea) as hex strings
            return obj.hex()
        return super().default(obj)


def chunked_postgres_reader(
    conn: Any, table_name: str, columns: list[str], batch_size: int = 10000
) -> Generator[list[dict[str, Any]], None, None]:
    """
    Generates batches of rows from a Postgres table using a server-side cursor
    to keep memory consumption low even with millions of rows.
    """
    columns_str = ", ".join(columns)
    cursor_name = f"migrate_cursor_{table_name}"

    # We use a named server-side cursor to stream rows
    with conn.cursor(name=cursor_name, cursor_factory=DictCursor) as srv_cur:
        srv_cursor = srv_cur
        srv_cursor.itersize = batch_size
        query = f"SELECT {columns_str} FROM {table_name}"
        srv_cursor.execute(query)

        while True:
            rows = srv_cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(row) for row in rows]


def migrate_table(
    pg_conn: Any,
    db: Any,
    table_name: str,
    batch_size: int = 10000,
    use_fallback_store: bool = False,
    fallback_store_dict: dict[str, bytes] = None,
) -> None:
    """
    Extracts rows from PostgreSQL, transforms them, and loads them into RocksDB.
    """
    schema = ROCKSDB_TABLE_SCHEMAS.get(table_name)
    if not schema:
        print(
            f"[-] Error: Table '{table_name}' has no defined schema mapping in rocksdb_schema.py."
        )
        return

    print(
        f"[*] Starting migration for table: '{table_name}' (Columns: {len(schema.columns)})..."
    )

    total_migrated = 0
    batch_index = 0

    for row_batch in chunked_postgres_reader(
        pg_conn, table_name, schema.columns, batch_size
    ):
        batch_index += 1

        # We use a write batch in RocksDB for high write throughput
        if HAS_ROCKSDICT and not use_fallback_store:
            write_batch = rocksdict.WriteBatch()
        else:
            write_batch = None

        for row in row_batch:
            # Construct composite primary key values
            pk_vals = tuple(row.get(pk) for pk in schema.primary_keys)

            if any(val is None for val in pk_vals):
                print(
                    f"[!] Warning: Row in '{table_name}' contains null in primary keys {schema.primary_keys}. Skipping."
                )
                continue

            # Convert row to serialized JSON bytes
            key = get_rocksdb_key(table_name, pk_vals)
            value = json.dumps(row, cls=CustomEncoder).encode("utf-8")

            if write_batch:
                # Add to high-speed batch
                write_batch[key] = value
            elif use_fallback_store and fallback_store_dict is not None:
                # Add to dictionary fallback
                fallback_store_dict[key.decode("utf-8")] = value
            else:
                db[key] = value

        # Commit batch
        if write_batch:
            db.write(write_batch)

        total_migrated += len(row_batch)
        print(
            f"    [+] Batch {batch_index}: Migrated {len(row_batch)} rows. (Total: {total_migrated})"
        )

    print(
        f"[+] Finished migrating table '{table_name}': {total_migrated} rows migrated.\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="High-performance PostgreSQL to RocksDB Migration tool."
    )
    parser.add_argument(
        "--pg-dsn",
        required=True,
        help="Postgres connection DSN (e.g. 'dbname=synapse user=postgres')",
    )
    parser.add_argument(
        "--rocksdb-path",
        default="rocksdb_experimental_store",
        help="Path to RocksDB database directory",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for ETL streaming and commits",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to migrate (default: all mapped tables)",
    )

    args = parser.parse_args()

    # 1. Connect to PostgreSQL
    try:
        pg_conn = psycopg2.connect(args.pg_dsn)
        print("[+] Connected to PostgreSQL successfully.")
    except Exception as e:
        print(f"[-] Error connecting to PostgreSQL: {e}")
        sys.exit(1)

    # 2. Open RocksDB / Fallback
    db = None
    use_fallback_store = False
    fallback_store_dict: dict[str, bytes] = {}

    if HAS_ROCKSDICT:
        try:
            db = rocksdict.Rdict(args.rocksdb_path)
            print(f"[+] Opened RocksDB store at '{args.rocksdb_path}'.")
        except Exception as e:
            print(
                f"[!] Failed to open RocksDB via rocksdict: {e}. Falling back to file-backed dict."
            )
            use_fallback_store = True
    else:
        print("[!] rocksdict not installed. Using file-backed mock JSON fallback.")
        use_fallback_store = True

    fallback_file = f"{args.rocksdb_path}.json"

    # 3. Determine tables to migrate
    tables_to_migrate = (
        args.tables if args.tables else list(ROCKSDB_TABLE_SCHEMAS.keys())
    )

    # 4. Migrate
    try:
        for table in tables_to_migrate:
            migrate_table(
                pg_conn=pg_conn,
                db=db,
                table_name=table,
                batch_size=args.batch_size,
                use_fallback_store=use_fallback_store,
                fallback_store_dict=fallback_store_dict,
            )

        # Write back fallback store if active
        if use_fallback_store:
            print(f"[*] Saving fallback dictionary to {fallback_file}...")
            try:
                # Decodes back to UTF-8 strings for JSON storage
                data_to_save = {
                    k: v.decode("utf-8") for k, v in fallback_store_dict.items()
                }
                with open(fallback_file, "w") as f:
                    json.dump(data_to_save, f, indent=2)
                print(f"[+] Saved {len(data_to_save)} items to fallback store.")
            except Exception as e:
                print(f"[-] Error saving fallback JSON file: {e}")

        print("[+] Migration completed successfully!")

    except KeyboardInterrupt:
        print("\n[-] Migration aborted by user.")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
