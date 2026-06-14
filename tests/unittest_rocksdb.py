#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

import os
import unittest
from synapse.storage.engines.rocksdb import connect


class TestRocksdbDBAPI2(unittest.TestCase):
    def setUp(self):
        # We ensure we start with a clean mock database state
        self.db_file = "rocksdb_experimental_store.json"
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

        self.conn = connect()
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

    def test_insert_and_select_pk(self):
        # Insert a user
        self.cursor.execute(
            "INSERT INTO users (name, password_hash, admin) VALUES (?, ?, ?)",
            ("alice", "hashed_pw_alice", 1),
        )
        self.conn.commit()

        # Query back via direct PK lookup
        self.cursor.execute(
            "SELECT name, password_hash, admin FROM users WHERE name = ?",
            ("alice",),
        )
        row = self.cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row, ("alice", "hashed_pw_alice", 1))

    def test_insert_and_select_scan(self):
        # Insert multiple users
        self.cursor.execute(
            "INSERT INTO users (name, password_hash, admin) VALUES (?, ?, ?)",
            ("alice", "hashed_pw_alice", 1),
        )
        self.cursor.execute(
            "INSERT INTO users (name, password_hash, admin) VALUES (?, ?, ?)",
            ("bob", "hashed_pw_bob", 0),
        )
        self.conn.commit()

        # Query both via scan
        self.cursor.execute("SELECT name, admin FROM users")
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertIn(("alice", 1), rows)
        self.assertIn(("bob", 0), rows)

    def test_update(self):
        # Insert a user
        self.cursor.execute(
            "INSERT INTO users (name, password_hash, admin) VALUES (?, ?, ?)",
            ("alice", "hashed_pw_alice", 1),
        )
        self.conn.commit()

        # Update their admin status
        self.cursor.execute(
            "UPDATE users SET admin = ? WHERE name = ?",
            (0, "alice"),
        )
        self.conn.commit()

        # Verify update
        self.cursor.execute(
            "SELECT admin FROM users WHERE name = ?",
            ("alice",),
        )
        row = self.cursor.fetchone()
        self.assertEqual(row, (0,))

    def test_delete(self):
        # Insert a user
        self.cursor.execute(
            "INSERT INTO users (name, password_hash, admin) VALUES (?, ?, ?)",
            ("alice", "hashed_pw_alice", 1),
        )
        self.conn.commit()

        # Delete the user
        self.cursor.execute(
            "DELETE FROM users WHERE name = ?",
            ("alice",),
        )
        self.conn.commit()

        # Verify they are gone
        self.cursor.execute(
            "SELECT name FROM users WHERE name = ?",
            ("alice",),
        )
        row = self.cursor.fetchone()
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
