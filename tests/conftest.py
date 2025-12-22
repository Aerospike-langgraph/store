import os
import pytest
import aerospike
@pytest.fixture(scope="session")
def client():
    """Create a single Aerospike client shared across tests."""
    host = os.getenv("AEROSPIKE_HOST", "127.0.0.1")
    port = int(os.getenv("AEROSPIKE_PORT", "3000"))
    cfg = {"hosts": [(host, port)]}
    try:
        c = aerospike.client(cfg).connect()
        print("Connected to Aerospike :heavy_check_mark:")
    except aerospike.exception.AerospikeError as e:
        pytest.skip(f"Could not connect to Aerospike at {host}:{port}: {e}")
    yield c
    try:
        c.close()
    except Exception:
        pass
@pytest.fixture(scope="session")
def namespace():
    """Default namespace for testing."""
    return "test"  # works with Docker default config
@pytest.fixture()
def store(client, namespace):
    """Create a fresh AerospikeStore instance for tests."""
    from langgraph.store.aerospike.base import AerospikeStore  # adjust import if needed
    return AerospikeStore(
        client=client,
        namespace=namespace,
        set="store_test",
    )

@pytest.fixture()
def store_with_vector(client, namespace):
    """Create an AerospikeStore with vector search capabilities."""
    from langgraph.store.aerospike.base import AerospikeStore
    
    # Simple embedding function for testing
    def simple_embed(texts: list[str]) -> list[list[float]]:
        """Simple embedding function for testing (returns dummy vectors)."""
        embeddings = []
        for text in texts:
            # Simple deterministic embedding: use character codes
            vec = [float(ord(c) % 10) / 10.0 for c in (text + "0" * 128)[:128]]
            embeddings.append(vec)
        return embeddings
    
    return AerospikeStore(
        client=client,
        namespace=namespace,
        set="store_vector_test",
        index={
            "dims": 128,
            "embed": simple_embed,
            "fields": ["$"]  # Embed entire document by default
        }
    )