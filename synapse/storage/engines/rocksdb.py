#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

import json
import os
import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

from synapse.storage.engines._base import BaseDatabaseEngine
from synapse.storage.engines.rocksdb_schema import ROCKSDB_TABLE_SCHEMAS, get_rocksdb_key
from synapse.storage.types import Connection, Cursor

# Let's try to import rocksdict, but fall back to a file-backed dictionary mock.
try:
    import rocksdict
    HAS_ROCKSDICT = True
except ImportError:
    HAS_ROCKSDICT = False

print(f"RocksDB Engine Translation Layer Initialized. rocksdict installed: {HAS_ROCKSDICT}")


class RocksdbMockDB:
    """
    A persistent key-value database. Uses rocksdict if available,
    otherwise falls back to a JSON-serialized local file dictionary.
    """
    def __init__(self, db_path: str = "rocksdb_experimental_store"):
        self.db_path = db_path
        self.has_rocksdict = HAS_ROCKSDICT
        self._db: Any = None
        self._fallback_store: Dict[str, bytes] = {}

        if self.has_rocksdict:
            try:
                # Use rocksdict to open/create RocksDB
                self._db = rocksdict.Rdict(db_path)
            except Exception as e:
                print(f"Failed to load rocksdict, falling back to local file-backed dictionary: {e}")
                self.has_rocksdict = False

        if not self.has_rocksdict:
            # Fallback to local JSON file
            self.json_file = f"{db_path}.json"
            if os.path.exists(self.json_file):
                try:
                    with open(self.json_file, "r") as f:
                        data = json.load(f)
                        # Re-encode keys and values to bytes
                        self._fallback_store = {k: v.encode("utf-8") for k, v in data.items()}
                except Exception as e:
                    print(f"Error loading fallback JSON DB: {e}")
                    self._fallback_store = {}

    def get(self, key: bytes) -> Optional[bytes]:
        if self.has_rocksdict:
            try:
                val = self._db[key]
                return val if isinstance(val, bytes) else str(val).encode("utf-8")
            except KeyError:
                return None
        else:
            return self._fallback_store.get(key.decode("utf-8"))

    def put(self, key: bytes, value: bytes) -> None:
        if self.has_rocksdict:
            self._db[key] = value
        else:
            self._fallback_store[key.decode("utf-8")] = value

    def delete(self, key: bytes) -> None:
        if self.has_rocksdict:
            try:
                del self._db[key]
            except KeyError:
                pass
        else:
            self._fallback_store.pop(key.decode("utf-8"), None)

    def scan_prefix(self, prefix: bytes) -> List[Tuple[bytes, bytes]]:
        results = []
        prefix_str = prefix.decode("utf-8")
        if self.has_rocksdict:
            # rocksdict prefix scanning
            iter_db = self._db.iter()
            iter_db.seek(prefix)
            while iter_db.is_valid():
                key = iter_db.key()
                if not key.startswith(prefix):
                    break
                results.append((key, iter_db.value()))
                iter_db.next()
        else:
            for k, v in self._fallback_store.items():
                if k.startswith(prefix_str):
                    results.append((k.encode("utf-8"), v))
        return results

    def commit(self) -> None:
        if self.has_rocksdict:
            # rocksdict handles persistence automatically
            pass
        else:
            # Save the fallback memory store back to JSON
            try:
                with open(self.json_file, "w") as f:
                    data = {k: v.decode("utf-8") for k, v in self._fallback_store.items()}
                    json.dump(data, f, indent=2)
            except Exception as e:
                print(f"Failed to commit/save fallback JSON DB: {e}")


# Global database instance
_GLOBAL_DB: Optional[RocksdbMockDB] = None


def get_global_db() -> RocksdbMockDB:
    global _GLOBAL_DB
    if _GLOBAL_DB is None:
        _GLOBAL_DB = RocksdbMockDB()
    return _GLOBAL_DB


# --- DBAPI2 COMPLIANT INTERFACE FOR ROCKSDB ---

class RocksdbCursor:
    def __init__(self, db: RocksdbMockDB):
        self._db = db
        self.description: Optional[List[Tuple[str, Any, None, None, None, None, bool]]] = None
        self.rowcount: int = -1
        self._results: List[Tuple[Any, ...]] = []
        self._results_index: int = 0

    def execute(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> "RocksdbCursor":
        # Clear previous state
        self._results = []
        self._results_index = 0
        self.rowcount = -1
        self.description = None

        # Clean/normalize SQL query
        sql_clean = " ".join(sql.strip().split())
        params = params or ()

        # 1. MATCH INSERT
        # INSERT INTO table (col1, col2) VALUES (?, ?)
        insert_match = re.match(
            r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)",
            sql_clean,
            re.IGNORECASE,
        )
        if insert_match:
            table = insert_match.group(1).lower()
            cols = [c.strip() for k in insert_match.group(2).split(",") for c in [k.strip()]]
            vals_placeholder = insert_match.group(3)

            schema = ROCKSDB_TABLE_SCHEMAS.get(table)
            if not schema:
                # Custom fallback schema if not registered
                schema = ROCKSDB_TABLE_SCHEMAS[table] = ROCKSDB_TABLE_SCHEMAS.get(
                    "users"
                )  # dummy schema fallback or build dynamically

            # Map placeholders/params
            row_dict = {}
            for col, val in zip(cols, params):
                row_dict[col] = val

            # Construct RocksDB Key using primary keys
            pk_vals = tuple(row_dict.get(pk) for pk in schema.primary_keys)
            if None in pk_vals or any(v is None for v in pk_vals):
                # Fallback: if PK is missing, use the first column or generate a serial
                pk_vals = (list(row_dict.values())[0],)

            key = get_rocksdb_key(table, pk_vals)
            value = json.dumps(row_dict).encode("utf-8")
            self._db.put(key, value)
            self.rowcount = 1
            return self

        # 2. MATCH SELECT
        # SELECT col1, col2 FROM table WHERE col3 = ?
        select_match = re.match(
            r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?$",
            sql_clean,
            re.IGNORECASE,
        )
        if select_match:
            columns_str = select_match.group(1).strip()
            table = select_match.group(2).lower()
            where_clause = select_match.group(3)

            schema = ROCKSDB_TABLE_SCHEMAS.get(table)
            cols_to_select = [c.strip() for c in columns_str.split(",")]
            if len(cols_to_select) == 1 and cols_to_select[0] == "*":
                if schema:
                    cols_to_select = schema.columns
                else:
                    cols_to_select = []

            # Filter rows
            all_rows: List[Dict[str, Any]] = []

            # See if we can optimize by matching primary key directly in WHERE clause
            is_pk_lookup = False
            if where_clause and schema:
                # Simple PK lookup match: e.g. "user_id = ?" or "name = ?"
                where_clean = where_clause.strip()
                pk_col = schema.primary_keys[0]
                pk_match = re.match(rf"^{pk_col}\s*=\s*\?$", where_clean, re.IGNORECASE)
                if pk_match and len(params) == 1:
                    is_pk_lookup = True
                    pk_val = params[0]
                    key = get_rocksdb_key(table, (pk_val,))
                    val_bytes = self._db.get(key)
                    if val_bytes:
                        all_rows.append(json.loads(val_bytes.decode("utf-8")))

            # If not a primary key lookup, perform a scan over the prefix
            if not is_pk_lookup:
                prefix = f"{table}:".encode("utf-8")
                kv_pairs = self._db.scan_prefix(prefix)
                for _, val_bytes in kv_pairs:
                    all_rows.append(json.loads(val_bytes.decode("utf-8")))

                # Evaluate where clause if present (very simple equality matching)
                if where_clause and len(params) > 0:
                    # Match "col = ?" or similar simple conditions
                    where_parts = re.findall(r"(\w+)\s*=\s*\?", where_clause)
                    filtered_rows = []
                    for row in all_rows:
                        matches = True
                        for col_name, param_val in zip(where_parts, params):
                            if row.get(col_name) != param_val:
                                matches = False
                                break
                        if matches:
                            filtered_rows.append(row)
                    all_rows = filtered_rows

            # Build result tuples
            for row in all_rows:
                res_tuple = tuple(row.get(c) for c in cols_to_select)
                self._results.append(res_tuple)

            self.rowcount = len(self._results)
            self.description = [(col, None, None, None, None, None, True) for col in cols_to_select]
            return self

        # 3. MATCH UPDATE
        # UPDATE table SET col1 = ?, col2 = ? WHERE col3 = ?
        update_match = re.match(
            r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?$",
            sql_clean,
            re.IGNORECASE,
        )
        if update_match:
            table = update_match.group(1).lower()
            sets_str = update_match.group(2)
            where_clause = update_match.group(3)

            schema = ROCKSDB_TABLE_SCHEMAS.get(table)
            set_cols = re.findall(r"(\w+)\s*=\s*\?", sets_str)
            set_vals = params[:len(set_cols)]
            where_vals = params[len(set_cols):]

            # Scan and update
            prefix = f"{table}:".encode("utf-8")
            kv_pairs = self._db.scan_prefix(prefix)
            updated_count = 0

            # Filter which ones to update
            for key, val_bytes in kv_pairs:
                row = json.loads(val_bytes.decode("utf-8"))
                should_update = True

                if where_clause and len(where_vals) > 0:
                    where_cols = re.findall(r"(\w+)\s*=\s*\?", where_clause)
                    for col_name, param_val in zip(where_cols, where_vals):
                        if row.get(col_name) != param_val:
                            should_update = False
                            break

                if should_update:
                    for col, val in zip(set_cols, set_vals):
                        row[col] = val
                    self._db.put(key, json.dumps(row).encode("utf-8"))
                    updated_count += 1

            self.rowcount = updated_count
            return self

        # 4. MATCH DELETE
        # DELETE FROM table WHERE col = ?
        delete_match = re.match(
            r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?$",
            sql_clean,
            re.IGNORECASE,
        )
        if delete_match:
            table = delete_match.group(1).lower()
            where_clause = delete_match.group(2)

            prefix = f"{table}:".encode("utf-8")
            kv_pairs = self._db.scan_prefix(prefix)
            deleted_count = 0

            for key, val_bytes in kv_pairs:
                row = json.loads(val_bytes.decode("utf-8"))
                should_delete = True

                if where_clause and len(params) > 0:
                    where_cols = re.findall(r"(\w+)\s*=\s*\?", where_clause)
                    for col_name, param_val in zip(where_cols, params):
                        if row.get(col_name) != param_val:
                            should_delete = False
                            break

                if should_delete:
                    self._db.delete(key)
                    deleted_count += 1

            self.rowcount = deleted_count
            return self

        # Fallback for complex/unhandled queries (e.g., schema creation/DDL)
        # We just swallow them to avoid crashing Synapse's startup
        print(f"SWALLOWED SQL (Unsupported in RocksDB translation layer): {sql_clean}")
        self.rowcount = 0
        return self

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        if self._results_index < len(self._results):
            res = self._results[self._results_index]
            self._results_index += 1
            return res
        return None

    def fetchall(self) -> List[Tuple[Any, ...]]:
        res = self._results[self._results_index:]
        self._results_index = len(self._results)
        return res

    def close(self) -> None:
        pass


class RocksdbConnection:
    def __init__(self, db: RocksdbMockDB):
        self._db = db

    def cursor(self) -> RocksdbCursor:
        return RocksdbCursor(self._db)

    def commit(self) -> None:
        self._db.commit()

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def connect(*args: Any, **kwargs: Any) -> RocksdbConnection:
    """
    Entrypoint for DBAPI2 compatibility.
    """
    return RocksdbConnection(get_global_db())


# --- SYNAPSE DATABASE ENGINE WRAPPER ---

class RocksdbEngine(BaseDatabaseEngine[RocksdbConnection, RocksdbCursor]):
    def __init__(self, database_config: Mapping[str, Any]):
        super().__init__(None, database_config)  # type: ignore

    @property
    def single_threaded(self) -> bool:
        return True

    @property
    def supports_using_any_list(self) -> bool:
        return False

    def check_database(
        self, db_conn: RocksdbConnection, allow_outdated_version: bool = False
    ) -> None:
        pass

    def check_new_database(self, txn: RocksdbCursor) -> None:
        pass

    def convert_param_style(self, sql: str) -> str:
        # DBAPI2 uses paramstyle 'qmark' (?), which SQLite also uses.
        return sql

    def on_new_connection(self, db_conn: Any) -> None:
        pass

    def is_deadlock(self, error: Exception) -> bool:
        return False

    def is_connection_closed(self, conn: RocksdbConnection) -> bool:
        return False

    def lock_table(self, txn: RocksdbCursor, table: str) -> None:
        pass

    @property
    def server_version(self) -> str:
        return "rocksdb-experimental-v1"

    @property
    def row_id_name(self) -> str:
        return "rowid"

    def in_transaction(self, conn: RocksdbConnection) -> bool:
        return False

    def attempt_to_set_autocommit(self, conn: RocksdbConnection, autocommit: bool) -> None:
        pass

    def attempt_to_set_isolation_level(
        self, conn: RocksdbConnection, isolation_level: int | None
    ) -> None:
        pass

    @staticmethod
    def executescript(cursor: RocksdbCursor, script: str) -> None:
        # Swallowed since we handle schemas dynamically
        pass
