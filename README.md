# LangGraph Aerospike Store

Persistent key-value store for LangGraph with **vector search capabilities**. Provides long-term memory that persists across threads and conversations, with support for semantic search using embeddings.

## Features

- ✅ **Key-Value Storage**: Store and retrieve data with hierarchical namespaces
- ✅ **Vector Search**: Semantic similarity search using embeddings
- ✅ **Metadata Filtering**: Filter results by exact match or comparison operators
- ✅ **TTL Support**: Automatic expiration of items
- ✅ **Async Support**: Full async/await support
- ✅ **Hybrid Search**: Combine vector similarity with metadata filters

## Quick start

### 1. Install dependencies
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt 
```

### 2. Start Aerospike
```bash
docker compose up -d
```

### 3. Basic Usage (Key-Value Store)

```python
import aerospike
from langgraph.store.aerospike import AerospikeStore

# Connect to Aerospike
client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()

# Create store
store = AerospikeStore(
    client=client,
    namespace="test",
    set="my_store"
)

# Store data
store.put(("users", "123"), "preferences", {"theme": "dark", "language": "en"})

# Retrieve data
item = store.get(("users", "123"), "preferences")
print(item.value)  # {"theme": "dark", "language": "en"}

# Search with filters
results = store.search(
    ("users",),
    filter={"theme": "dark"},
    limit=10
)
```

### 4. Vector Search Usage

```python
import aerospike
from langgraph.store.aerospike import AerospikeStore

# Connect to Aerospike
client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()

# Create store with vector support
store = AerospikeStore(
    client=client,
    namespace="test",
    set="documents",
    index={
        "dims": 1536,
        "embed": "openai:text-embedding-3-small",  # or custom function
        "fields": ["text"]  # fields to embed (default: ["$"] for entire doc)
    }
)

# Store documents (vectors generated automatically)
store.put(("docs",), "doc1", {"text": "Python is a programming language", "category": "tech"})
store.put(("docs",), "doc2", {"text": "Machine learning with Python", "category": "ai"})
store.put(("docs",), "doc3", {"text": "JavaScript for web development", "category": "web"})

# Semantic search
results = store.search(
    ("docs",),
    query="artificial intelligence programming",  # natural language query
    limit=5
)

for result in results:
    print(f"Score: {result.score:.3f} - {result.value['text']}")

# Hybrid search (vector + metadata)
results = store.search(
    ("docs",),
    query="Python coding",
    filter={"category": "ai"},  # only return AI category
    limit=3
)
```

### 5. Custom Embedding Function

```python
from openai import OpenAI

client_openai = OpenAI()

def my_embed_function(texts: list[str]) -> list[list[float]]:
    response = client_openai.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )
    return [e.embedding for e in response.data]

store = AerospikeStore(
    client=aerospike_client,
    namespace="test",
    set="my_docs",
    index={
        "dims": 1536,
        "embed": my_embed_function,
        "fields": ["title", "content"]  # index specific fields
    }
)
```

## Implementation Details

- **Core implementation**: `langgraph/store/aerospike/base.py`
- **Vector storage**: Uses Aerospike's native vector operations (`vector_write`, `vector_distance`)
- **Search method**: Currently uses brute-force k-NN (scans all records and computes distances)
  - Suitable for small-to-medium datasets (< 10K vectors)
  - Future: Will use Aerospike Vector Secondary Index for large-scale deployments

## Configuration

### Environment Variables
- `AEROSPIKE_HOST`: Aerospike server host (default: `127.0.0.1`)
- `AEROSPIKE_PORT`: Aerospike server port (default: `3000`)
- `AEROSPIKE_NAMESPACE`: Namespace to use (default: `test`)

### TTL Configuration
```python
store = AerospikeStore(
    client=client,
    namespace="test",
    set="my_store",
    ttl_config={
        "default_ttl": 60.0,  # minutes
        "refresh_on_read": True  # refresh TTL on reads
    }
)
```

### Vector Index Configuration
```python
index_config = {
    "dims": 1536,                              # embedding dimensions
    "embed": "openai:text-embedding-3-small",  # or custom function
    "fields": ["$"]                            # fields to embed (default: entire document)
}

# Common embedding models:
# - "openai:text-embedding-3-small" (1536 dims)
# - "openai:text-embedding-3-large" (3072 dims)
# - "cohere:embed-english-v3.0" (1024 dims)
```

## Tests

Requires a running Aerospike instance (`docker compose up -d`).

### Run all tests:
```bash
pytest
```

### Test Coverage:
- `tests/test_aerospike_store_v1.py`: Basic store operations
- `tests/test_aeropsike_store.py`: Advanced filtering and operations
- `tests/test_vector_search.py`: Vector search functionality
  - Vector write and search
  - Hybrid search (vector + metadata filters)
  - Custom field indexing
  - Index=False behavior

### Run specific tests:
```bash
# Test vector search only
pytest tests/test_vector_search.py -v

# Test basic store operations
pytest tests/test_aerospike_store_v1.py -v
```

## API Reference

### Store Methods

- `put(namespace, key, value, index=None, ttl=None)`: Store an item
- `get(namespace, key, refresh_ttl=None)`: Retrieve an item
- `delete(namespace, key)`: Delete an item
- `search(namespace_prefix, query=None, filter=None, limit=10, offset=0)`: Search items
- `list_namespaces(prefix=None, suffix=None, max_depth=None, limit=100)`: List namespaces

### Async Methods

All methods have async equivalents: `aput`, `aget`, `adelete`, `asearch`, `alist_namespaces`

## Performance Notes

- **Vector Search**: Current implementation uses brute-force distance computation
  - Suitable for: < 10K vectors
  - For larger datasets: Aerospike Vector Secondary Index (coming soon)
- **Metadata Search**: Uses Aerospike expression filters for efficient filtering
- **TTL**: Native Aerospike TTL support for automatic expiration

## Future Enhancements

- [ ] Vector Secondary Index integration for large-scale vector search
- [ ] Batch vector operations for improved throughput
- [ ] Support for different distance metrics (cosine, dot product, etc.)
- [ ] Vector index creation and management API

## Summary

AerospikeStore provides a robust, production-ready key-value store with semantic search capabilities for LangGraph applications. It combines Aerospike's performance and scalability with LangGraph's memory abstractions and vector search support.