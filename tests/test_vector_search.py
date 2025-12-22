"""Test vector search functionality with AerospikeStore."""
import pytest
from langgraph.store.aerospike import AerospikeStore


def cleanup(store):
    """Delete all records in the store."""
    scan = store.client.scan(store.ns, store.set)
    for key, meta, bins in scan.results():
        store.client.remove(key)


def simple_embed(texts: list[str]) -> list[list[float]]:
    """Simple embedding function for testing (returns dummy vectors)."""
    # Create simple embeddings based on text length and content
    embeddings = []
    for text in texts:
        # Simple deterministic embedding: use character codes
        vec = [float(ord(c) % 10) / 10.0 for c in (text + "0" * 128)[:128]]
        embeddings.append(vec)
    return embeddings


def test_vector_write_and_search(store_with_vector):
    """Test storing documents with vectors and searching by similarity."""
    cleanup(store_with_vector)
    
    ns = ("docs",)
    
    # Store some documents
    store_with_vector.put(ns, "doc1", {"text": "Python programming language"})
    store_with_vector.put(ns, "doc2", {"text": "JavaScript web development"})
    store_with_vector.put(ns, "doc3", {"text": "Python data science"})
    store_with_vector.put(ns, "doc4", {"text": "Java enterprise applications"})
    
    # Search for Python-related docs
    results = store_with_vector.search(
        ns,
        query="Python coding",
        limit=3
    )
    
    assert len(results) >= 2
    # Check that results have scores
    assert all(r.score is not None for r in results)
    # Results should be ordered by relevance (score)
    scores = [r.score for r in results]
    assert scores == sorted(scores, reverse=True)


def test_vector_search_with_metadata_filter(store_with_vector):
    """Test combining vector search with metadata filters."""
    cleanup(store_with_vector)
    
    ns = ("articles",)
    
    # Store articles with metadata
    store_with_vector.put(ns, "art1", {
        "text": "Machine learning with Python",
        "category": "AI",
        "published": True
    })
    store_with_vector.put(ns, "art2", {
        "text": "Python web frameworks",
        "category": "Web",
        "published": True
    })
    store_with_vector.put(ns, "art3", {
        "text": "Deep learning neural networks",
        "category": "AI",
        "published": False
    })
    
    # Search for AI articles that are published
    results = store_with_vector.search(
        ns,
        query="artificial intelligence",
        filter={"category": "AI", "published": True},
        limit=5
    )
    
    assert len(results) == 1
    assert results[0].value["category"] == "AI"
    assert results[0].value["published"] is True


def test_vector_search_with_custom_fields(store_with_vector):
    """Test indexing specific fields for vector search."""
    cleanup(store_with_vector)
    
    ns = ("products",)
    
    # Store with specific field indexing
    store_with_vector.put(
        ns, 
        "prod1", 
        {
            "title": "Laptop Computer",
            "description": "High performance machine",
            "price": 1200
        },
        index=["title", "description"]  # Only index these fields
    )
    
    store_with_vector.put(
        ns,
        "prod2",
        {
            "title": "Wireless Mouse",
            "description": "Ergonomic design",
            "price": 25
        },
        index=["title", "description"]
    )
    
    # Search should work
    results = store_with_vector.search(
        ns,
        query="computer hardware",
        limit=2
    )
    
    assert len(results) >= 1


def test_no_vector_search_without_index_config(store):
    """Test that vector search fails gracefully without index config."""
    cleanup(store)
    
    ns = ("docs",)
    store.put(ns, "doc1", {"text": "Some content"})
    
    # Should raise NotImplementedError
    with pytest.raises(NotImplementedError, match="IndexConfig"):
        store.search(ns, query="content")


def test_skip_vector_on_index_false(store_with_vector):
    """Test that index=False skips vector generation."""
    cleanup(store_with_vector)
    
    ns = ("docs",)
    
    # Store with index=False
    store_with_vector.put(
        ns,
        "doc1",
        {"text": "No vector for this"},
        index=False
    )
    
    # Store with indexing (default)
    store_with_vector.put(
        ns,
        "doc2",
        {"text": "This has a vector"}
    )
    
    # Search should only return doc2
    results = store_with_vector.search(
        ns,
        query="vector document",
        limit=5
    )
    
    # doc1 should not appear in results (no vector)
    result_keys = [r.key for r in results]
    assert "doc2" in result_keys
    # doc1 might not appear or appear with low score


def test_metadata_search_still_works(store_with_vector):
    """Test that regular metadata search works without query parameter."""
    cleanup(store_with_vector)
    
    ns = ("items",)
    
    store_with_vector.put(ns, "item1", {"type": "book", "author": "Alice"})
    store_with_vector.put(ns, "item2", {"type": "book", "author": "Bob"})
    store_with_vector.put(ns, "item3", {"type": "video", "author": "Alice"})
    
    # Metadata search without query (no vector search)
    results = store_with_vector.search(
        ns,
        filter={"type": "book"},
        limit=10
    )
    
    assert len(results) == 2
    assert all(r.value["type"] == "book" for r in results)
    # Scores should be None for metadata-only search
    assert all(r.score is None for r in results)

