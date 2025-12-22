#!/usr/bin/env python3
"""
Example demonstrating vector search with AerospikeStore.

This example shows:
1. Setting up a store with vector support
2. Storing documents with automatic embedding
3. Semantic search by similarity
4. Hybrid search (vector + metadata filters)
"""

import aerospike
from langgraph.store.aerospike import AerospikeStore


def simple_embed(texts: list[str]) -> list[list[float]]:
    """Simple embedding function for demo (replace with real embeddings in production)."""
    embeddings = []
    for text in texts:
        # Create a simple deterministic embedding based on text
        # In production, use OpenAI, Cohere, or other embedding models
        vec = [float(ord(c) % 10) / 10.0 for c in (text + "0" * 128)[:128]]
        embeddings.append(vec)
    return embeddings


def main():
    # Connect to Aerospike
    print("Connecting to Aerospike...")
    client = aerospike.client({"hosts": [("127.0.0.1", 3000)]}).connect()
    print("✓ Connected\n")
    
    # Create store with vector support
    print("Creating store with vector support...")
    store = AerospikeStore(
        client=client,
        namespace="test",
        set="vector_demo",
        index={
            "dims": 128,
            "embed": simple_embed,
            "fields": ["text"]  # embed the 'text' field
        }
    )
    print("✓ Store created\n")
    
    # Store some documents
    print("Storing documents...")
    documents = [
        {"text": "Python is a high-level programming language", "category": "programming"},
        {"text": "Machine learning is a subset of artificial intelligence", "category": "ai"},
        {"text": "JavaScript is used for web development", "category": "programming"},
        {"text": "Deep learning uses neural networks", "category": "ai"},
        {"text": "React is a JavaScript library for building user interfaces", "category": "web"},
        {"text": "Data science involves analyzing large datasets", "category": "data"},
        {"text": "Natural language processing is a branch of AI", "category": "ai"},
    ]
    
    for i, doc in enumerate(documents):
        store.put(("docs",), f"doc{i}", doc)
        print(f"  ✓ Stored doc{i}: {doc['text'][:50]}...")
    
    print(f"\n✓ Stored {len(documents)} documents\n")
    
    # Example 1: Semantic search
    print("=" * 70)
    print("Example 1: Semantic Search")
    print("=" * 70)
    query = "artificial intelligence and neural networks"
    print(f"Query: '{query}'\n")
    
    results = store.search(
        ("docs",),
        query=query,
        limit=3
    )
    
    print(f"Top {len(results)} results:")
    for i, result in enumerate(results, 1):
        print(f"\n{i}. Score: {result.score:.4f}")
        print(f"   Text: {result.value['text']}")
        print(f"   Category: {result.value['category']}")
    
    # Example 2: Hybrid search (vector + metadata filter)
    print("\n" + "=" * 70)
    print("Example 2: Hybrid Search (Vector + Metadata Filter)")
    print("=" * 70)
    query = "coding and software"
    print(f"Query: '{query}'")
    print("Filter: category = 'programming'\n")
    
    results = store.search(
        ("docs",),
        query=query,
        filter={"category": "programming"},
        limit=5
    )
    
    print(f"Top {len(results)} results:")
    for i, result in enumerate(results, 1):
        print(f"\n{i}. Score: {result.score:.4f}")
        print(f"   Text: {result.value['text']}")
        print(f"   Category: {result.value['category']}")
    
    # Example 3: Metadata-only search (no vector)
    print("\n" + "=" * 70)
    print("Example 3: Metadata-Only Search (No Vector)")
    print("=" * 70)
    print("Filter: category = 'ai'\n")
    
    results = store.search(
        ("docs",),
        filter={"category": "ai"},
        limit=10
    )
    
    print(f"Found {len(results)} results:")
    for i, result in enumerate(results, 1):
        print(f"\n{i}. Text: {result.value['text']}")
        print(f"   Category: {result.value['category']}")
        print(f"   Score: {result.score} (None for metadata-only search)")
    
    # Cleanup
    print("\n" + "=" * 70)
    print("Cleaning up...")
    for i in range(len(documents)):
        store.delete(("docs",), f"doc{i}")
    print("✓ Cleanup complete")
    
    client.close()
    print("\n✓ Demo complete!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except aerospike.exception.AerospikeError as e:
        print(f"\n❌ Aerospike error: {e}")
        print("Make sure Aerospike is running: docker compose up -d")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

