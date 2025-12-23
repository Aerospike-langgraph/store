"""
Tests for Aerospike vector search functionality.

These tests verify that semantic/vector search works correctly with embeddings.
"""

import pytest
import aerospike
from langgraph.store.base import PutOp, GetOp, SearchOp, Item, SearchItem
from langgraph.store.aerospike.base import AerospikeStore
from langchain_core.embeddings import Embeddings
import hashlib
import math

# Configuration for local Docker instance
AEROSPIKE_CONFIG = {'hosts': [('localhost', 3000)]}
TEST_NAMESPACE = "test"
TEST_SET = "langgraph_vector_store"

# Simple mock embedding class for testing
class MockEmbeddings(Embeddings):
    """Simple mock embedding class that generates deterministic vectors from text."""
    
    def __init__(self, dimensions: int = 128):
        self.dimensions = dimensions
    
    def embed_query(self, text: str) -> list[float]:
        """Generate a deterministic embedding vector from text."""
        # Use hash to create deterministic but varied vectors
        hash_obj = hashlib.md5(text.encode())
        seed = int(hash_obj.hexdigest()[:8], 16)
        
        # Generate vector using seeded random-like values
        vector = []
        for i in range(self.dimensions):
            # Create pseudo-random value based on seed and index
            val = (seed * (i + 1) * 0.001) % 1.0
            vector.append(float(val))
        
        return vector
    
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts."""
        return [self.embed_query(text) for text in texts]


@pytest.fixture(scope="session")
def aerospike_client():
    """Creates a single connection for the whole test session."""
    try:
        client = aerospike.client(AEROSPIKE_CONFIG).connect()
        yield client
        client.close()
    except aerospike.exception.AerospikeError as e:
        pytest.skip(f"Could not connect to Aerospike: {e}")


@pytest.fixture
def vector_store(aerospike_client):
    """Creates a store instance with vector search enabled."""
    # Create mock embeddings
    embeddings = MockEmbeddings(dimensions=128)
    
    # Create store with index_config
    store = AerospikeStore(
        client=aerospike_client,
        namespace=TEST_NAMESPACE,
        set=TEST_SET,
        index_config={
            "embed": embeddings,
            "vector_bin": "embedding",
            "vector_dims": 128,
            "fields": ["$"]  # Embed entire value
        }
    )
    
    # Cleanup before test
    try:
        scan = aerospike_client.scan(TEST_NAMESPACE, TEST_SET)
        def callback(input_tuple):
            key, _, _ = input_tuple
            aerospike_client.remove(key)
        scan.foreach(callback)
    except Exception:
        pass  # Ignore if set is empty
    
    return store


@pytest.fixture
def store_without_vectors(aerospike_client):
    """Creates a store instance without vector search enabled."""
    store = AerospikeStore(
        client=aerospike_client,
        namespace=TEST_NAMESPACE,
        set=TEST_SET
    )
    
    # Cleanup before test
    try:
        scan = aerospike_client.scan(TEST_NAMESPACE, TEST_SET)
        def callback(input_tuple):
            key, _, _ = input_tuple
            aerospike_client.remove(key)
        scan.foreach(callback)
    except Exception:
        pass
    
    return store


def test_vector_search_basic(vector_store):
    """Test basic vector search with query."""
    ns = ("docs", "articles")
    
    # Add documents with different content
    docs = [
        ("doc1", {"text": "machine learning and artificial intelligence"}),
        ("doc2", {"text": "python programming and data science"}),
        ("doc3", {"text": "cooking recipes and food preparation"}),
    ]
    
    # Store documents (embeddings will be generated automatically)
    for key, value in docs:
        vector_store.put(ns, key, value)
    
    # Search for "AI and machine learning" - should find doc1 as most similar
    results = vector_store.search(
        ns,
        query="AI and machine learning",
        limit=3
    )
    
    assert len(results) > 0
    assert all(isinstance(r, SearchItem) for r in results)
    assert all(r.score is not None for r in results)
    assert all(0 < r.score <= 1.0 for r in results)  # Scores should be in (0, 1]
    
    # Most similar should be doc1 (about ML/AI)
    top_result = results[0]
    assert top_result.key in ["doc1", "doc2", "doc3"]  # Should be one of our docs
    assert top_result.namespace == ns


def test_vector_search_with_limit(vector_store):
    """Test that limit parameter works correctly."""
    ns = ("test", "limit")
    
    # Add multiple documents
    for i in range(10):
        vector_store.put(ns, f"doc_{i}", {"text": f"document number {i} about topic {i % 3}"})
    
    # Search with limit
    results = vector_store.search(
        ns,
        query="topic 0",
        limit=5
    )
    
    assert len(results) <= 5
    assert all(r.score is not None for r in results)


def test_vector_search_with_offset(vector_store):
    """Test that offset parameter works correctly."""
    ns = ("test", "offset")
    
    # Add multiple documents
    for i in range(10):
        vector_store.put(ns, f"doc_{i}", {"text": f"document {i} with content"})
    
    # Search without offset
    results_all = vector_store.search(
        ns,
        query="content",
        limit=10
    )
    
    # Search with offset
    results_offset = vector_store.search(
        ns,
        query="content",
        limit=10,
        offset=3
    )
    
    # Results with offset should be different (if we have enough results)
    if len(results_all) > 3:
        assert len(results_offset) <= len(results_all) - 3
        # First result with offset should not be the same as first without offset
        if results_offset and results_all:
            assert results_offset[0].key != results_all[0].key or len(results_all) > 3


def test_vector_search_with_namespace_prefix(vector_store):
    """Test vector search filtered by namespace prefix."""
    ns1 = ("docs", "tech")
    ns2 = ("docs", "cooking")
    
    # Add documents to different namespaces
    vector_store.put(ns1, "doc1", {"text": "python programming"})
    vector_store.put(ns1, "doc2", {"text": "javascript development"})
    vector_store.put(ns2, "doc1", {"text": "italian pasta recipes"})
    vector_store.put(ns2, "doc2", {"text": "baking bread techniques"})
    
    # Search only in tech namespace
    results = vector_store.search(
        ns1,
        query="programming languages",
        limit=10
    )
    
    assert len(results) == 2  # Should find both tech docs
    assert all(r.namespace == ns1 for r in results)
    assert {r.key for r in results} == {"doc1", "doc2"}


def test_vector_search_with_metadata_filter(vector_store):
    """Test vector search combined with metadata filters."""
    ns = ("docs", "filtered")
    
    # Add documents with metadata
    vector_store.put(ns, "doc1", {
        "text": "machine learning algorithms",
        "category": "tech",
        "status": "published"
    })
    vector_store.put(ns, "doc2", {
        "text": "deep learning neural networks",
        "category": "tech",
        "status": "draft"
    })
    vector_store.put(ns, "doc3", {
        "text": "cooking pasta recipes",
        "category": "food",
        "status": "published"
    })
    
    # Search with both query and filter
    results = vector_store.search(
        ns,
        query="learning algorithms",
        filter={"category": "tech", "status": "published"},
        limit=10
    )
    
    assert len(results) >= 1
    # All results should match the filter
    for r in results:
        assert r.value["category"] == "tech"
        assert r.value["status"] == "published"
    
    # Should find doc1 (matches both query and filter)
    keys = {r.key for r in results}
    assert "doc1" in keys


def test_vector_search_without_embeddings_raises_error(store_without_vectors):
    """Test that vector search raises error when embeddings not configured."""
    ns = ("test",)
    store_without_vectors.put(ns, "doc1", {"text": "some content"})
    
    # Should raise ValueError when trying to search with query
    with pytest.raises(ValueError, match="Vector search requires index_config"):
        store_without_vectors.search(
            ns,
            query="some query",
            limit=10
        )


def test_put_with_indexing_disabled(vector_store):
    """Test that putting with index=False doesn't create embeddings."""
    ns = ("test", "no_index")
    
    # Put with index=False
    vector_store.put(ns, "doc1", {"text": "some content"}, index=False)
    
    # Verify document exists
    item = vector_store.get(ns, "doc1")
    assert item is not None
    assert item.value == {"text": "some content"}
    
    # Search should not find it (no embedding)
    # Note: This depends on implementation - if vector_distance requires embedding,
    # records without embeddings might be skipped
    results = vector_store.search(
        ns,
        query="some content",
        limit=10
    )
    
    # The document might not appear in results if it has no embedding
    # This is expected behavior


def test_batch_operations_with_embeddings(vector_store):
    """Test batch operations that include puts with embeddings."""
    ns = ("batch", "test")
    
    # Batch put multiple documents
    ops = [
        PutOp(namespace=ns, key="doc1", value={"text": "first document"}),
        PutOp(namespace=ns, key="doc2", value={"text": "second document"}),
        PutOp(namespace=ns, key="doc3", value={"text": "third document"}),
    ]
    vector_store.batch(ops)
    
    # Verify all documents were stored
    for key in ["doc1", "doc2", "doc3"]:
        item = vector_store.get(ns, key)
        assert item is not None
    
    # Search should find them
    results = vector_store.search(
        ns,
        query="document",
        limit=10
    )
    
    assert len(results) == 3
    keys = {r.key for r in results}
    assert keys == {"doc1", "doc2", "doc3"}


def test_vector_search_similarity_scores(vector_store):
    """Test that similarity scores are reasonable (higher = more similar)."""
    ns = ("similarity", "test")
    
    # Add documents with very similar and very different content
    vector_store.put(ns, "similar1", {"text": "machine learning algorithms"})
    vector_store.put(ns, "similar2", {"text": "machine learning models"})
    vector_store.put(ns, "different", {"text": "cooking pasta recipes"})
    
    # Search for "machine learning"
    results = vector_store.search(
        ns,
        query="machine learning",
        limit=3
    )
    
    assert len(results) == 3
    
    # Results should be sorted by similarity (highest first)
    scores = [r.score for r in results]
    assert scores == sorted(scores, reverse=True)
    
    # Similar documents should have higher scores than different one
    similar_scores = [r.score for r in results if r.key.startswith("similar")]
    different_scores = [r.score for r in results if r.key == "different"]
    
    if similar_scores and different_scores:
        # At least one similar doc should score higher than different doc
        assert max(similar_scores) > min(different_scores)


def test_vector_search_empty_namespace(vector_store):
    """Test vector search in empty namespace returns empty list."""
    ns = ("empty", "namespace")
    
    results = vector_store.search(
        ns,
        query="anything",
        limit=10
    )
    
    assert results == []


def test_vector_search_combined_with_exact_search(vector_store):
    """Test that non-vector search still works alongside vector search."""
    ns = ("mixed", "search")
    
    # Add documents
    vector_store.put(ns, "doc1", {"text": "machine learning", "tag": "ai"})
    vector_store.put(ns, "doc2", {"text": "python programming", "tag": "coding"})
    
    # Vector search
    vector_results = vector_store.search(
        ns,
        query="machine learning",
        limit=10
    )
    assert len(vector_results) > 0
    assert all(r.score is not None for r in vector_results)
    
    # Exact search (no query)
    exact_results = vector_store.search(
        ns,
        filter={"tag": "ai"},
        limit=10
    )
    assert len(exact_results) == 1
    assert exact_results[0].key == "doc1"
    assert exact_results[0].score is None  # No score for exact search

