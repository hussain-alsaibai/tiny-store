# tiny-store

> **Filesystem-backed key-value store with TTL, search, pagination, and watch.
> Zero dependencies. Composable with fast-cache for L1+L2 tiering.**

`tiny-store` is a lightweight, crash-safe KV store backed by the filesystem.
It stores each key as a separate JSON file with atomic writes, supports TTL expiration,
prefix/regex search, cursor-based pagination, JSONL import/export, and file-watching.
Composable with `fast-cache` for a memory-L1 + disk-L2 cache tier.

```bash
pip install tiny-store
# or: copy tiny_store.py into your project (MIT licensed)
```

## Features

| Feature | Details |
|---------|---------|
| **Atomic writes** | Rename-after-write on POSIX — crash-safe |
| **TTL support** | Per-key or global time-to-live with auto-expiry |
| **WAL mode** | Write-ahead log for guaranteed durability |
| **Search** | Prefix scan, regex match, and value filtering |
| **Pagination** | Cursor-based iteration over large keyspaces |
| **Watch** | File-system polling observer for key changes |
| **Import/Export** | JSONL bulk load and dump |
| **Tiered mode** | L1 (memory dict) + L2 (disk) composable wrapper |
| **Zero dependencies** | Stdlib only — no pip install chain |

## Quick Start

```python
from tiny_store import Store

store = Store("./data")

# Basic operations
store.put("user:123", {"name": "Alice", "tier": "pro"}, ttl=3600)
data = store.get("user:123")
print(data)  # {'name': 'Alice', 'tier': 'pro'}

# Atomic update
store.update("counter", lambda n: (n or 0) + 1)

# Search by prefix
for key, value in store.scan(prefix="user:"):
    print(key, value)

# Cursor pagination
for page in store.paginate(page_size=100):
    process(page["keys"], page["values"])
    if not page["next_cursor"]:
        break

# Export / import
store.export_jsonl("backup.jsonl")
store.import_jsonl("backup.jsonl")

# Clean expired entries
n = store.clean_expired()
print(f"Removed {n} expired entries")
```

## Tiered Cache: Memory + Disk

Compose with `fast-cache` for L1 (hot) + L2 (warm) tiering:

```python
from tiny_store import TieredStore, Store
from fast_cache import LRUCache

# L1: in-memory LRU (from fast-cache)
l1 = LRUCache(max_size=1000)

# L2: disk-backed (from tiny-store)
l2 = Store("./data")

tiered = TieredStore(l2, hot=l1._data, hot_limit=1000)

# Now get() checks L1 → L2 → None, and put() writes both
tiered.put("session:active", {"user_id": 42}, ttl=300)
val = tiered.get("session:active")  # served from L1 if hot
```

## Directory Layout

```
<root>/
├── .index           # JSON: key → {mtime, size} (fast enumeration)
├── .pending/         # WAL: in-flight atomic writes
└── <key>.json       # One file per key (key = URL-safe base64 encoded)
```

Each entry file:
```json
{
  "value": {"name": "Alice", "tier": "pro"},
  "created_at": 1753651200.0,
  "updated_at": 1753651200.0,
  "expires_at": 1753654800.0
}
```

## API Reference

### Store

```python
from tiny_store import Store

store = Store(
    root="./data",          # Storage directory
    default_ttl=3600,       # Default TTL for all entries (seconds)
    wal=True,               # Write-ahead log (atomic commits)
    lazy_load=True,         # Only load entries on get()
    index_enabled=True,    # Maintain on-disk key index for fast enumeration
)
```

**Core operations:**
```python
store.put(key, value, ttl=None, if_not_exists=False)  # → bool
store.get(key, default=None, update_ttl=None)          # → value
store.delete(key)                                       # → bool
store.has(key)                                          # → bool
store.update(key, fn)                                   # → new_value
store.touch(key, ttl=None)                             # → bool
```

**Iteration:**
```python
store.keys()       # → Iterator[str]
store.values()     # → Iterator[Any]
store.items()     # → Iterator[(str, Any)]
store.scan(prefix="", regex=None, limit=0)  # → Iterator[(str, Any)]
store.count(prefix="")                                  # → int
store.paginate(page_size=100, prefix="")  # → Iterator[Page]
```

**TTL / Expiry:**
```python
store.clean_expired()         # Remove all expired entries → int
store.expires_in(key)         # Seconds until expiry → float | None
```

**Import / Export:**
```python
store.export_jsonl(path)      # → int (count)
store.import_jsonl(path)       # → int (count)
store.dump()                  # → dict (all key→value)
store.load(data, overwrite=True)  # → int (count)
```

**Watch:**
```python
for event in store.watch(key="session:42", debounce=0.5):
    print(event)  # {"type": "put"|"delete", "key": "...", "value": ...}
```

**Admin:**
```python
store.size()         # Active (non-expired) entry count
store.disk_size()    # Bytes on disk
store.clear()        # Delete all entries → int
store.stats()        # Full stats dict
```

### TieredStore

```python
from tiny_store import TieredStore, Store

tiered = TieredStore(
    store=Store("./data"),
    hot={},               # L1 memory dict
    hot_limit=1000,       # Evict after N entries
)
```

## CLI

```bash
# Set the store root
export TINY_STORE_ROOT="./data"

# Store a value
python tiny_store.py put "user:1" '{"name": "Alice"}' --ttl 3600

# Retrieve it
python tiny_store.py get "user:1"

# List with prefix
python tiny_store.py list --prefix "user:"

# Scan with regex
python tiny_store.py scan --prefix "session:" --regex "session:[0-9]+"

# Clean expired
python tiny_store.py clean

# Stats
python tiny_store.py stats

# Clear all
python tiny_store.py clear
```

## Architecture

```
tiny_store.py
├── Entry                     # Dataclass: key/value/metadata
├── Store                     # Main KV store
│   ├── put()                # Atomic write (WAL + rename)
│   ├── get()                # Lazy load with TTL check
│   ├── delete()             # Remove entry + index
│   ├── scan()              # Prefix / regex scan
│   ├── paginate()          # Cursor-based iteration
│   ├── clean_expired()     # TTL garbage collection
│   ├── export_jsonl()      # Bulk dump
│   ├── import_jsonl()      # Bulk load
│   └── watch()             # File-system polling observer
├── TieredStore              # L1 (memory) + L2 (disk) wrapper
│   ├── get()               # L1 → L2 → default
│   ├── put()               # Write both tiers
│   └── _promote()          # Evict L1 → L2
└── CLI
    └── main()              # python tiny_store.py <cmd>
```

## Use Cases

| Scenario | Why tiny-store |
|----------|---------------|
| **Agent memory** | Survive restarts with disk persistence |
| **Feature flags** | Time-based flag rollout with automatic expiry |
| **Session store** | TTL-backed session data without Redis |
| **Config snapshots** | Store config versions, roll back by key |
| **Webhook deduplication** | Store event IDs with TTL to prevent replay |
| **Rate limit counters** | Per-IP counters with sliding window TTL |
| **LLM cache L2** | Composable with fast-cache for L1+L2 tiering |

## Ecosystem

`tiny-store` is part of the tiny-* toolkit:

| Tool | Role |
|------|------|
| `fast-cache` | L1 hot cache (memory) |
| `tiny-store` | L2 warm store (disk) |
| `tiny-log` | Structured audit logging |
| `tiny-config` | Config precedence |
| `tiny-llm` | LLM client (for agents that need it) |

## License

MIT — copy, modify, use in production, no attribution required.
