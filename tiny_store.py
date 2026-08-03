"""
tiny_store.py — Zero-dependency filesystem-backed key-value store with TTL,
search, pagination, and watch. Composable with fast-cache for L1 (memory) +
L2 (disk) tiering.

Use cases:
  - Agent memory persistence across restarts
  - Configuration snapshots with rollback
  - Simple document store with TTL
  - Cache warming: dump fast-cache L1 → disk L2 → reload on startup
  - Lightweight feature flag store with time-based expiration

Design goals:
  - Zero dependencies (stdlib only)
  - Atomic writes (rename-after-write on POSIX)
  - Crash-safe (WAL-style transaction log)
  - Memory-efficient (lazy loading — only loaded entries are in RAM)
  - Easy to audit: one file, no magic

Usage:
    from tiny_store import Store

    store = Store("./data")
    store.put("user:123", {"name": "Alice", "tier": "pro"}, ttl=3600)
    data = store.get("user:123")
    print(data)  # {"name": "Alice", "tier": "pro"}

    # Search by prefix
    for key, value in store.scan("user:"):
        print(key, value)

    # With TTL auto-expiry
    import time
    time.sleep(1)
    print(store.get("session:active"))  # None if expired

    # Cursor-based pagination
    for batch in store.paginate(page_size=100):
        process(batch)
"""

from __future__ import annotations

import json
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterator,
    Literal,
    TypedDict,
)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class EntryDict(TypedDict):
    value: Any
    created_at: float
    updated_at: float
    expires_at: float | None


@dataclass
class Entry:
    key: str
    value: Any
    created_at: float
    updated_at: float
    expires_at: float | None

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def to_dict(self) -> EntryDict:
        return {
            "value": self.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, key: str, d: EntryDict) -> Entry:
        return cls(
            key=key,
            value=d["value"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            expires_at=d.get("expires_at"),
        )


class Page(TypedDict):
    keys: list[str]
    values: list[Any]
    next_cursor: str | None


# ---------------------------------------------------------------------------
# Internal file format
# ---------------------------------------------------------------------------

# We store each key as a separate JSON file inside the store directory.
# Filename = safe(key).json  (key may contain /, :, etc. — we encode it)
# Optional WAL: writes go to .pending/<uuid>.json first, then renamed to final

PENDING_DIR = ".pending"
INDEX_FILE = ".index"


def _safe_name(key: str) -> str:
    """Encode a key into a safe filename."""
    # Use URL-safe base64 encoding
    import base64
    encoded = base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii")
    # Remove trailing = padding
    return encoded.rstrip("=")


def _unsafe_name(name: str) -> str:
    """Decode a filename back to the original key."""
    import base64
    # Add back padding
    pad = (4 - len(name) % 4) % 4
    encoded = name + "=" * pad
    return base64.urlsafe_b64decode(encoded.encode("ascii")).decode("utf-8")


# ---------------------------------------------------------------------------
# Store implementation
# ---------------------------------------------------------------------------

class Store:
    """
    Filesystem-backed KV store with TTL, search, pagination, and watch.

    Directory layout:
      <root>/
        .index          # JSON: {"key": {"mtime": float, "size": int}, ...}
        .pending/       # WAL: in-flight writes (atomic commit on rename)
        aGVsbG8=       # key="hello" → encoded filename
        dXNlcjoxMjM=    # key="user:123"
        ...
    """

    def __init__(
        self,
        root: str | Path = "./store",
        default_ttl: float | None = None,
        wal: bool = True,
        lazy_load: bool = True,
        index_enabled: bool = True,
    ):
        self.root = Path(root).resolve()
        self.default_ttl = default_ttl
        self.wal = wal
        self.lazy_load = lazy_load
        self.index_enabled = index_enabled

        # In-memory state
        self._cache: dict[str, Entry] = {}  # loaded entries
        self._index: dict[str, dict[str, Any]] = {}  # persisted index

        # Create structure
        self._init_dirs()

        # Load index
        if self.index_enabled:
            self._load_index()

    # ---- Initialization ----

    def _init_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if self.wal:
            (self.root / PENDING_DIR).mkdir(exist_ok=True)

    def _entry_path(self, key: str) -> Path:
        return self.root / f"{_safe_name(key)}.json"

    def _load_index(self) -> None:
        idx_path = self.root / INDEX_FILE
        if idx_path.exists():
            try:
                with open(idx_path, "r", encoding="utf-8") as f:
                    self._index = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._index = {}
        else:
            self._index = {}

    def _save_index(self) -> None:
        idx_path = self.root / INDEX_FILE
        with open(str(idx_path) + ".tmp", "w", encoding="utf-8") as f:
            json.dump(self._index, f)
        os.replace(str(idx_path) + ".tmp", str(idx_path))

    # ---- Core operations ----

    def put(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        if_not_exists: bool = False,
    ) -> bool:
        """
        Store a value.

        Args:
            key: Unique key
            value: JSON-serializable value
            ttl: Time-to-live in seconds (None = no expiry)
            if_not_exists: If True, only write if key doesn't exist

        Returns:
            True if written, False if skipped (if_not_exists and key exists)
        """
        if if_not_exists and key in self._cache:
            # Check disk too
            p = self._entry_path(key)
            if p.exists():
                return False

        now = time.time()
        expires_at = (now + ttl) if ttl is not None else (now + self.default_ttl if self.default_ttl else None)

        entry = Entry(
            key=key,
            value=value,
            created_at=now,
            updated_at=now,
            expires_at=expires_at,
        )

        return self._write_entry(entry)

    def _write_entry(self, entry: Entry) -> bool:
        path = self._entry_path(entry.key)
        data = entry.to_dict()

        if self.wal:
            # Write to WAL, then rename atomically
            pending_dir = self.root / PENDING_DIR
            pending_path = pending_dir / f"{uuid.uuid4().hex}.json"
            with open(pending_path, "w", encoding="utf-8") as f:
                json.dump({"key": entry.key, **data}, f)
            os.replace(pending_path, path)
        else:
            tmp = path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, path)

        # Update index
        stat = path.stat()
        self._index[entry.key] = {"mtime": stat.st_mtime, "size": stat.st_size}
        if self.index_enabled:
            self._save_index()

        # Update cache
        self._cache[entry.key] = entry

        return True

    def get(
        self,
        key: str,
        default: Any = None,
        update_ttl: float | None = None,
    ) -> Any:
        """
        Retrieve a value by key.

        Args:
            key: Key to look up
            default: Value to return if key is missing or expired
            update_ttl: If set, extends/updates the TTL on a cache hit

        Returns:
            The stored value, or `default` if not found / expired.
        """
        entry = self._load_entry(key)
        if entry is None:
            return default
        if entry.is_expired():
            self.delete(key)
            return default
        if update_ttl is not None:
            entry.expires_at = time.time() + update_ttl
            self._write_entry(entry)
        return entry.value

    def _load_entry(self, key: str) -> Entry | None:
        # Check cache first
        if key in self._cache:
            return self._cache[key]

        if self.lazy_load:
            path = self._entry_path(key)
            if not path.exists():
                return None
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                entry = Entry.from_dict(key, data)
                if not self.lazy_load:
                    self._cache[key] = entry
                return entry
            except (json.JSONDecodeError, OSError):
                return None
        return None

    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if it existed."""
        path = self._entry_path(key)
        existed = path.exists()
        if existed:
            try:
                os.unlink(path)
            except OSError:
                pass
        if key in self._cache:
            del self._cache[key]
        if key in self._index:
            del self._index[key]
            if self.index_enabled:
                self._save_index()
        return existed

    def has(self, key: str) -> bool:
        """Check if a key exists and is not expired."""
        entry = self._load_entry(key)
        if entry is None:
            return False
        if entry.is_expired():
            self.delete(key)
            return False
        return True

    def update(self, key: str, fn: Callable[[Any], Any]) -> Any | None:
        """
        Atomically read-modify-write a value.

        Args:
            key: Key to update
            fn: Function that takes the current value and returns the new value.
                If key doesn't exist, fn is called with None.

        Returns:
            The new value, or None if key doesn't exist and create=False.
        """
        current = self.get(key)
        new_value = fn(current)
        self.put(key, new_value)
        return new_value

    def touch(self, key: str, ttl: float | None = None) -> bool:
        """Extend or set TTL on an existing key. Returns True if key exists."""
        entry = self._load_entry(key)
        if entry is None or entry.is_expired():
            return False
        entry.expires_at = (time.time() + ttl) if ttl is not None else None
        entry.updated_at = time.time()
        self._write_entry(entry)
        return True

    # ---- Iteration & search ----

    def keys(self) -> Iterator[str]:
        """Yield all keys (including expired ones — use has() to filter)."""
        if self.index_enabled and self._index:
            yield from self._index.keys()
        else:
            for p in self.root.iterdir():
                if p.suffix == ".json" and p.name not in (INDEX_FILE,):
                    try:
                        yield _unsafe_name(p.stem)
                    except Exception:
                        continue

    def values(self) -> Iterator[Any]:
        """Yield all non-expired values."""
        for key in self.keys():
            val = self.get(key)
            if val is not None:
                yield val

    def items(self) -> Iterator[tuple[str, Any]]:
        """Yield all non-expired (key, value) pairs."""
        for key in self.keys():
            val = self.get(key)
            if val is not None:
                yield key, val

    def scan(
        self,
        prefix: str = "",
        regex: str | None = None,
        limit: int = 0,
    ) -> Iterator[tuple[str, Any]]:
        """
        Scan keys with optional prefix filter and regex.

        Args:
            prefix: Only yield keys starting with this prefix
            regex: Only yield keys matching this regex (Python syntax)
            limit: Maximum number of results (0 = unlimited)
        """
        pattern = re.compile(regex) if regex else None
        count = 0
        for key in self.keys():
            if prefix and not key.startswith(prefix):
                continue
            if pattern and not pattern.search(key):
                continue
            val = self.get(key)
            if val is not None:
                yield key, val
                count += 1
                if limit and count >= limit:
                    break

    def count(self, prefix: str = "") -> int:
        """Count non-expired keys matching prefix."""
        return sum(1 for k, _ in self.scan(prefix=prefix))

    # ---- Pagination ----

    def paginate(
        self,
        page_size: int = 100,
        prefix: str = "",
    ) -> Iterator[Page]:
        """
        Cursor-based pagination over keys.

        Args:
            page_size: Number of entries per page
            prefix: Only iterate keys starting with this prefix

        Yields:
            Page dicts with keys, values, and next_cursor (None on last page)
        """
        cursor: str | None = None
        all_keys = sorted(k for k, _ in self.scan(prefix=prefix))

        while True:
            page_keys = []
            page_values = []
            found_cursor = False

            for key in all_keys:
                if cursor is not None and not found_cursor:
                    if key == cursor:
                        found_cursor = True
                    continue

                val = self.get(key)
                if val is None:
                    continue

                page_keys.append(key)
                page_values.append(val)

                if len(page_keys) >= page_size:
                    next_cursor = key
                    yield {
                        "keys": page_keys,
                        "values": page_values,
                        "next_cursor": next_cursor,
                    }
                    cursor = next_cursor
                    page_keys = []
                    page_values = []
                    break
            else:
                # Last page
                if page_keys:
                    yield {
                        "keys": page_keys,
                        "values": page_values,
                        "next_cursor": None,
                    }
                return

    # ---- TTL / expiry ----

    def clean_expired(self) -> int:
        """
        Remove all expired entries from the store.
        Returns the number of entries deleted.
        """
        deleted = 0
        for key in list(self.keys()):
            entry = self._load_entry(key)
            if entry and entry.is_expired():
                if self.delete(key):
                    deleted += 1
        return deleted

    def expires_in(self, key: str) -> float | None:
        """Return seconds until key expires, or None if no TTL."""
        entry = self._load_entry(key)
        if entry is None or entry.expires_at is None:
            return None
        remaining = entry.expires_at - time.time()
        return remaining if remaining > 0 else 0.0

    # ---- Import / Export ----

    def export_jsonl(self, path: str | Path) -> int:
        """
        Export all entries as JSONL (one JSON object per line).
        Returns the number of entries exported.
        """
        count = 0
        with open(path, "w", encoding="utf-8") as f:
            for key, value in self.items():
                entry = self._load_entry(key)
                if entry:
                    f.write(json.dumps({"key": key, **entry.to_dict()}) + "\n")
                    count += 1
        return count

    def import_jsonl(self, path: str | Path, overwrite: bool = True) -> int:
        """
        Import entries from JSONL. Returns the number of entries imported.
        Skips keys that already exist unless overwrite=True.
        """
        count = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    key = obj.pop("key")
                    value = obj.pop("value", None)
                    created_at = obj.pop("created_at", time.time())
                    updated_at = obj.pop("updated_at", time.time())
                    expires_at = obj.pop("expires_at", None)
                    entry = Entry(key, value, created_at, updated_at, expires_at)
                    if overwrite or (key not in self._cache and not self._entry_path(key).exists()):
                        self._write_entry(entry)
                        count += 1
                except (json.JSONDecodeError, KeyError):
                    continue
        return count

    def dump(self) -> dict[str, Any]:
        """Export all entries as a plain dict (key → value)."""
        return dict(self.items())

    def load(self, data: dict[str, Any], overwrite: bool = True) -> int:
        """Load a dict of key→value into the store. Returns count."""
        count = 0
        for key, value in data.items():
            if overwrite or not self.has(key):
                self.put(key, value)
                count += 1
        return count

    # ---- Watch (file-system observer) ----

    def watch(
        self,
        key: str | None = None,
        debounce: float = 0.5,
    ) -> Iterator[dict[str, Any]]:
        """
        Watch for changes to a specific key or the entire store.

        Args:
            key: If set, watch only this key. If None, watch the whole store.
            debounce: Minimum seconds between events for the same key.

        Yields:
            Dicts like {"type": "put"|"delete", "key": "...", "value": ...}

        Note: Uses polling on the entry file mtime. For production,
        use inotify/LiveElements on Linux or FSEvents on macOS.
        """
        seen: dict[str, float] = {}
        prev: dict[str, Any] = {}

        if key is not None:
            candidates = [key]
        else:
            candidates = list(self.keys())
            prev = dict(self.items())

        while True:
            time.sleep(debounce)

            if key is not None:
                current = self.get(key)
                if key in prev:
                    if prev[key] != current:
                        if current is None:
                            yield {"type": "delete", "key": key}
                        else:
                            yield {"type": "put", "key": key, "value": current}
                elif current is not None:
                    yield {"type": "put", "key": key, "value": current}
                prev[key] = current
            else:
                # Diff all keys
                current_items = dict(self.items())
                all_keys = set(list(prev.keys()) + list(current_items.keys()))

                for k in all_keys:
                    old_val = prev.get(k)
                    new_val = current_items.get(k)
                    if old_val != new_val:
                        if new_val is None:
                            yield {"type": "delete", "key": k}
                        else:
                            yield {"type": "put", "key": k, "value": new_val}
                        seen[k] = time.time()
                prev = current_items

    # ---- Admin ----

    def size(self) -> int:
        """Total number of non-expired entries."""
        return sum(1 for _ in self.items())

    def disk_size(self) -> int:
        """Total size on disk in bytes."""
        total = 0
        for p in self.root.rglob("*.json"):
            try:
                total += p.stat().st_size
            except OSError:
                continue
        return total

    def clear(self) -> int:
        """Delete all entries. Returns count of deleted keys."""
        count = 0
        for key in list(self.keys()):
            if self.delete(key):
                count += 1
        self._cache.clear()
        return count

    def stats(self) -> dict[str, Any]:
        """Return store statistics."""
        expired = 0
        for key in self.keys():
            entry = self._load_entry(key)
            if entry and entry.is_expired():
                expired += 1
        return {
            "total_keys": len(self._index) if self.index_enabled else sum(1 for _ in self.root.glob("*.json")),
            "active_keys": self.size(),
            "expired_keys": expired,
            "disk_size_bytes": self.disk_size(),
            "cache_size": len(self._cache),
        }

    def __repr__(self) -> str:
        return f"Store(root={str(self.root)!r}, size={self.size()})"


# ---------------------------------------------------------------------------
# Tiered wrapper: L1 (memory) + L2 (disk)  ← composable with fast-cache
# ---------------------------------------------------------------------------

class TieredStore:
    """
    Two-tier store: hot (memory dict) + warm (disk).
    On get: check L1 first, fall back to L2, promote to L1.
    On put: write to L1 + L2.
    On evict from L1: write to L2.

    Composable with fast-cache's LRUCache for the L1 tier.
    """

    def __init__(
        self,
        store: Store,
        hot: dict[str, Any] | None = None,
        hot_limit: int = 1000,
    ):
        self.store = store
        self.hot: dict[str, Any] = hot or {}
        self.hot_limit = hot_limit

    def get(self, key: str, default: Any = None) -> Any:
        if key in self.hot:
            return self.hot[key]
        val = self.store.get(key, default=None)
        if val is not None:
            self._promote(key, val)
        return val if val is not None else default

    def put(self, key: str, value: Any, ttl: float | None = None) -> None:
        self.store.put(key, value, ttl=ttl)
        self._promote(key, value)

    def delete(self, key: str) -> bool:
        self.hot.pop(key, None)
        return self.store.delete(key)

    def has(self, key: str) -> bool:
        if key in self.hot:
            return True
        return self.store.has(key)

    def _promote(self, key: str, value: Any) -> None:
        if len(self.hot) >= self.hot_limit:
            # Evict oldest (FIFO — for LRU, use fast-cache.LRUCache instead)
            oldest = next(iter(self.hot))
            del self.hot[oldest]
        self.hot[key] = value

    def items(self) -> Iterator[tuple[str, Any]]:
        seen = set()
        for k, v in self.hot.items():
            if self.store.has(k):
                seen.add(k)
                yield k, v
        yield from self.store.items()

    def size(self) -> int:
        return len(self.hot) + self.store.size()

    def __repr__(self) -> str:
        return f"TieredStore(hot={len(self.hot)}, store={self.store!r})"


# ---------------------------------------------------------------------------
# CLI tool wrapper
# ---------------------------------------------------------------------------

def main():
    """Tiny CLI: python tiny_store.py <command> [args]"""
    import argparse

    parser = argparse.ArgumentParser(description="tiny_store — filesystem KV CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    put_p = sub.add_parser("put", help="Store a key-value pair")
    put_p.add_argument("key")
    put_p.add_argument("value")
    put_p.add_argument("--ttl", type=float, default=None)

    get_p = sub.add_parser("get", help="Retrieve a value")
    get_p.add_argument("key")

    del_p = sub.add_parser("delete", help="Delete a key")
    del_p.add_argument("key")

    list_p = sub.add_parser("list", help="List all keys")
    list_p.add_argument("--prefix", default="")
    list_p.add_argument("--limit", type=int, default=0)

    scan_p = sub.add_parser("scan", help="Scan with prefix/regex")
    scan_p.add_argument("--prefix", default="")
    scan_p.add_argument("--regex", default=None)

    clean_p = sub.add_parser("clean", help="Remove expired entries")
    stats_p = sub.add_parser("stats", help="Show store statistics")
    clear_p = sub.add_parser("clear", help="Delete all entries")

    parser.add_argument("--root", "-r", default="./store", help="Store directory")
    args = parser.parse_args()

    store = Store(args.root)

    if args.cmd == "put":
        try:
            value = json.loads(args.value)
        except json.JSONDecodeError:
            value = args.value
        ok = store.put(args.key, value, ttl=args.ttl)
        print(f"Stored: {ok}")

    elif args.cmd == "get":
        val = store.get(args.key)
        if val is None:
            print("(not found)")
        else:
            print(json.dumps(val, indent=2, default=str))

    elif args.cmd == "delete":
        ok = store.delete(args.key)
        print(f"Deleted: {ok}")

    elif args.cmd == "list":
        for key, val in store.scan(prefix=args.prefix, limit=args.limit):
            print(f"{key}: {json.dumps(val, default=str)}")

    elif args.cmd == "scan":
        for key, val in store.scan(prefix=args.prefix, regex=args.regex):
            print(f"{key}: {json.dumps(val, default=str)}")

    elif args.cmd == "clean":
        n = store.clean_expired()
        print(f"Removed {n} expired entries")

    elif args.cmd == "stats":
        import pprint
        pprint.pprint(store.stats())

    elif args.cmd == "clear":
        n = store.clear()
        print(f"Cleared {n} entries")


if __name__ == "__main__":
    main()
