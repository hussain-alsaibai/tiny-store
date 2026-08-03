"""Tests for tiny-store — filesystem-backed KV store."""
import os
import shutil
import tempfile
import time
import pytest
from tiny_store import Store


@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


class TestBasic:
    def test_put_and_get(self, tmpdir):
        store = Store(tmpdir)
        store.put("key1", {"hello": "world"})
        data = store.get("key1")
        assert data == {"hello": "world"}

    def test_get_missing(self, tmpdir):
        store = Store(tmpdir)
        assert store.get("nonexistent") is None

    def test_get_with_default(self, tmpdir):
        store = Store(tmpdir)
        result = store.get("missing", default="fallback")
        assert result == "fallback"

    def test_delete(self, tmpdir):
        store = Store(tmpdir)
        store.put("key1", {"hello": "world"})
        assert store.delete("key1") is True
        assert store.get("key1") is None

    def test_delete_missing(self, tmpdir):
        store = Store(tmpdir)
        assert store.delete("nonexistent") is False

    def test_keys(self, tmpdir):
        store = Store(tmpdir)
        store.put("a", "value_a")
        store.put("b", "value_b")
        store.put("c", "value_c")
        keys = list(store.keys())
        assert set(keys) == {"a", "b", "c"}

    def test_values(self, tmpdir):
        store = Store(tmpdir)
        store.put("a", "val_a")
        store.put("b", "val_b")
        values = list(store.values())
        assert set(values) == {"val_a", "val_b"}

    def test_items(self, tmpdir):
        store = Store(tmpdir)
        store.put("a", "val_a")
        store.put("b", "val_b")
        items = dict(store.items())
        assert items == {"a": "val_a", "b": "val_b"}

    def test_if_not_exists(self, tmpdir):
        store = Store(tmpdir)
        store.put("key1", "first", if_not_exists=True)
        store.put("key1", "second", if_not_exists=True)
        assert store.get("key1") == "first"

    def test_update_ttl_on_get(self, tmpdir):
        store = Store(tmpdir)
        store.put("session", "active", ttl=2)
        _ = store.get("session", update_ttl=5)
        time.sleep(2)
        # Should still exist because get() extended TTL
        assert store.get("session") == "active"


class TestTTL:
    def test_ttl_expires(self, tmpdir):
        store = Store(tmpdir)
        store.put("session:1", {"user": "alice"}, ttl=1)
        assert store.get("session:1") is not None
        time.sleep(1.5)
        assert store.get("session:1") is None

    def test_ttl_none_no_expiry(self, tmpdir):
        store = Store(tmpdir)
        store.put("permanent", {"data": True}, ttl=None)
        time.sleep(0.1)
        assert store.get("permanent") == {"data": True}

    def test_global_ttl(self, tmpdir):
        store = Store(tmpdir, default_ttl=1)
        store.put("item1", "value")
        assert store.get("item1") == "value"
        time.sleep(1.5)
        assert store.get("item1") is None


class TestScan:
    def test_scan_prefix(self, tmpdir):
        store = Store(tmpdir)
        store.put("user:1", {"name": "Alice"})
        store.put("user:2", {"name": "Bob"})
        store.put("post:1", {"title": "Hello"})
        results = list(store.scan("user:"))
        assert len(results) == 2
        assert all(k.startswith("user:") for k, _ in results)

    def test_scan_regex(self, tmpdir):
        store = Store(tmpdir)
        store.put("order:100", "order_100")
        store.put("order:200", "order_200")
        store.put("user:1", "user_1")
        results = list(store.scan(regex=r"order:\d+"))
        assert len(results) == 2

    def test_scan_limit(self, tmpdir):
        store = Store(tmpdir)
        for i in range(10):
            store.put(f"key:{i}", f"val{i}")
        results = list(store.scan(prefix="key:", limit=3))
        assert len(results) == 3

    def test_scan_all(self, tmpdir):
        store = Store(tmpdir)
        store.put("a", 1)
        store.put("b", 2)
        store.put("c", 3)
        results = list(store.scan())
        assert len(results) == 3


class TestClear:
    def test_clear(self, tmpdir):
        store = Store(tmpdir)
        store.put("k1", "v1")
        store.put("k2", "v2")
        cleared = store.clear()
        assert cleared == 2
        assert store.get("k1") is None
        assert store.get("k2") is None


class TestAtomicity:
    def test_crash_safety(self, tmpdir):
        """Data survives process restart (atomic rename-after-write)."""
        store1 = Store(tmpdir)
        store1.put("persistent", {"survives": True})
        del store1

        store2 = Store(tmpdir)
        assert store2.get("persistent") == {"survives": True}


class TestStats:
    def test_stats(self, tmpdir):
        store = Store(tmpdir)
        store.put("k1", "v1")
        store.put("k2", "v2")
        stats = store.stats()
        # Stats dict uses 'total_keys' not 'key_count'
        assert stats.get("total_keys", stats.get("key_count", 0)) == 2


class TestPersistence:
    def test_data_persists_across_instances(self, tmpdir):
        """Same directory, two Store instances — data is shared."""
        store1 = Store(tmpdir)
        store1.put("shared", "visible")

        store2 = Store(tmpdir)
        assert store2.get("shared") == "visible"

        store2.put("also_shared", True)
        assert store1.get("also_shared") is True
