import tempfile
import shutil
import unittest
from synapse.synapse_rust import rocksdb_engine

class TestNativeRocksDBEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a temporary directory for the RocksDB test store
        cls.test_dir = tempfile.mkdtemp()
        
        # Initialize our native Rust engine
        print("Initializing native Rust RocksDB Engine...")
        rocksdb_engine.open_db(cls.test_dir)

    @classmethod
    def tearDownClass(cls):
        # Clean up the DB files after testing
        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def test_put_and_get(self):
        """Test that we can write and read back a value bypassing the GIL."""
        rocksdb_engine.put("user:@alice:example.com", '{"displayname": "Alice"}')
        
        # Retrieve the value
        result = rocksdb_engine.get("user:@alice:example.com")
        self.assertEqual(result, '{"displayname": "Alice"}')

    def test_get_nonexistent(self):
        """Test reading a key that does not exist."""
        result = rocksdb_engine.get("does_not_exist")
        self.assertIsNone(result)

    def test_scan_prefix(self):
        """Test scanning keys by prefix using the raw RocksDB iterator."""
        rocksdb_engine.put("room:1:state:A", "State A")
        rocksdb_engine.put("room:1:state:B", "State B")
        rocksdb_engine.put("room:2:state:C", "State C")
        
        results = rocksdb_engine.scan_prefix("room:1:state:")
        
        # We should only get the two keys belonging to room 1
        self.assertEqual(len(results), 2)
        keys = [k for k, v in results]
        self.assertIn("room:1:state:A", keys)
        self.assertIn("room:1:state:B", keys)
        self.assertNotIn("room:2:state:C", keys)

    def test_delete(self):
        """Test deleting a key."""
        rocksdb_engine.put("temp_key", "temp_value")
        self.assertEqual(rocksdb_engine.get("temp_key"), "temp_value")
        
        rocksdb_engine.delete("temp_key")
        self.assertIsNone(rocksdb_engine.get("temp_key"))

if __name__ == '__main__':
    unittest.main()