"""
Simple test to verify put and get operations with embeddings.
This test helps debug what's actually being stored in Aerospike.
"""

import pytest
import aerospike
from langgraph.store.aerospike.base import AerospikeStore
from langchain_core.embeddings import Embeddings
from langgraph.store.base import get_text_at_path
import hashlib

# Configuration for local Docker instance
AEROSPIKE_CONFIG = {'hosts': [('localhost', 3000)]}
TEST_NAMESPACE = "test"
TEST_SET = "langgraph_debug"

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


def test_debug_text_extraction(vector_store):
    """Debug what text is being extracted for embedding."""
    value = {"text": "This is a test document about AI and machine learning"}
    
    # Test what get_text_at_path returns
    print("\n=== Testing get_text_at_path ===")
    print(f"Value: {value}")
    print(f"Fields: {vector_store.fields}")
    
    # Test with the fields from the store
    texts = get_text_at_path(value, vector_store.fields)
    print(f"get_text_at_path returned: {texts}")
    print(f"Type: {type(texts)}")
    print(f"Length: {len(texts) if isinstance(texts, list) else 'N/A'}")
    
    # Test what _extract_text_for_embedding returns
    extracted = vector_store._extract_text_for_embedding(value, None)
    print(f"_extract_text_for_embedding returned: {repr(extracted)}")
    print(f"Type: {type(extracted)}")
    print(f"Truthy: {bool(extracted)}")
    
    # Test embedding generation
    if extracted:
        embedding = vector_store.embeddings.embed_query(extracted)
        print(f"Embedding generated: {len(embedding)} dimensions")
        print(f"First 5 values: {embedding[:5]}")
    else:
        print("WARNING: No text extracted, embedding would not be created!")


def test_put_with_embedding(vector_store, aerospike_client):
    """Test that put stores embeddings correctly."""
    namespace = ("test", "docs")
    key = "doc1"
    value = {"text": "This is a test document about AI and machine learning"}
    
    # Debug: Check what will be extracted
    print("\n=== Before put() ===")
    extracted = vector_store._extract_text_for_embedding(value, None)
    print(f"Text to embed: {repr(extracted)}")
    print(f"Will create embedding: {bool(extracted)}")
    
    # Put the document
    vector_store.put(namespace, key, value)
    
    # Directly check what's stored in Aerospike
    p_key = (TEST_NAMESPACE, TEST_SET, "|".join([*namespace, key]))
    
    try:
        _, _, bins = aerospike_client.get(p_key)
        
        print("\n=== What's stored in Aerospike ===")
        print(f"Key: {p_key}")
        print(f"Bins: {list(bins.keys())}")
        print(f"namespace: {bins.get('namespace')}")
        print(f"key: {bins.get('key')}")
        print(f"value: {bins.get('value')}")
        print(f"created_at: {bins.get('created_at')}")
        print(f"updated_at: {bins.get('updated_at')}")
        
        # Check if embedding bin exists
        embedding = bins.get("embedding")
        if embedding:
            print(f"embedding bin exists: {type(embedding)}")
            if isinstance(embedding, list):
                print(f"embedding length: {len(embedding)}")
                print(f"embedding first 5 values: {embedding[:5]}")
            else:
                print(f"embedding type: {type(embedding)}, value: {embedding}")
        else:
            print("WARNING: embedding bin NOT found!")
        
        # Verify embedding was stored
        print(f"embedding: {bins.get('embedding')}")
        assert "embedding" in bins, "Embedding bin should be present"
        assert isinstance(bins["embedding"], list), "Embedding should be a list"
        assert len(bins["embedding"]) == 128, "Embedding should have 128 dimensions"
        
    except aerospike.exception.AerospikeError as e:
        pytest.fail(f"Failed to get record from Aerospike: {e}")


def test_get_with_embedding(vector_store):
    """Test that get retrieves the stored data correctly."""
    namespace = ("test", "docs")
    key = "doc2"
    value = {"text": "Another test document about neural networks"}
    
    # Put the document
    vector_store.put(namespace, key, value)
    
    # Get it back
    item = vector_store.get(namespace, key)
    
    print("\n=== What's retrieved via get() ===")
    print(f"Item: {item}")
    if item:
        print(f"Item namespace: {item.namespace}")
        print(f"Item key: {item.key}")
        print(f"Item value: {item.value}")
        print(f"Item created_at: {item.created_at}")
        print(f"Item updated_at: {item.updated_at}")
    
    # Verify we got the data back
    assert item is not None, "Item should not be None"
    assert item.key == key, "Key should match"
    assert item.namespace == namespace, "Namespace should match"
    assert item.value == value, "Value should match"


def test_put_get_multiple(vector_store, aerospike_client):
    """Test putting and getting multiple documents."""
    namespace = ("test", "docs")
    documents = [
        ("doc1", {"text": "Machine learning is fascinating"}),
        ("doc2", {"text": "Deep learning uses neural networks"}),
        ("doc3", {"text": "Natural language processing is complex"}),
    ]
    
    # Put all documents
    for key, value in documents:
        vector_store.put(namespace, key, value)
    
    # Verify all are stored with embeddings
    for key, value in documents:
        p_key = (TEST_NAMESPACE, TEST_SET, "|".join([*namespace, key]))
        _, _, bins = aerospike_client.get(p_key)
        
        print(f"\n=== Document: {key} ===")
        print(f"Has embedding: {'embedding' in bins}")
        if "embedding" in bins:
            print(f"Embedding type: {type(bins['embedding'])}, length: {len(bins['embedding']) if isinstance(bins['embedding'], list) else 'N/A'}")
        
        assert "embedding" in bins, f"Document {key} should have embedding"
        assert isinstance(bins["embedding"], list), f"Document {key} embedding should be a list"
        assert len(bins["embedding"]) == 128, f"Document {key} embedding should have 128 dimensions"
        
        # Also test get()
        item = vector_store.get(namespace, key)
        assert item is not None, f"Should be able to get {key}"
        assert item.value == value, f"Value for {key} should match"


def test_put_without_indexing(vector_store, aerospike_client):
    """Test that put with index=False doesn't store embeddings."""
    namespace = ("test", "docs")
    key = "no_embedding_doc"
    value = {"text": "This should not have an embedding"}
    
    # Put with index=False
    vector_store.put(namespace, key, value, index=False)
    
    # Check what's stored
    p_key = (TEST_NAMESPACE, TEST_SET, "|".join([*namespace, key]))
    _, _, bins = aerospike_client.get(p_key)
    
    print("\n=== Document with index=False ===")
    print(f"Bins: {list(bins.keys())}")
    print(f"Has embedding: {'embedding' in bins}")
    
    # Should still have the data but no embedding
    assert "value" in bins, "Should have value bin"
    assert "embedding" not in bins, "Should NOT have embedding bin when index=False"