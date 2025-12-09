import pytest
import aerospike
from langgraph.store.base import PutOp, GetOp, SearchOp, ListNamespacesOp, Item
from langgraph.store.aerospike.base import AerospikeStore
from langgraph.store.base import MatchCondition
# Configuration for local Docker instance
AEROSPIKE_CONFIG = {'hosts': [('localhost', 3000)]}
TEST_NAMESPACE = "test"  # Aerospike default namespace is often 'test'
TEST_SET = "langgraph_store"
@pytest.fixture(scope="session")
def aerospike_client():
    """
    Creates a single connection for the whole test session.
    """
    client = aerospike.client(AEROSPIKE_CONFIG).connect()
    yield client
    client.close()
@pytest.fixture
def store(aerospike_client):
    """
    Creates the store instance and cleans up the set before each test.
    """
    store = AerospikeStore(
        client=aerospike_client,
        namespace=TEST_NAMESPACE,
        set=TEST_SET
    )
    # --- Cleanup / Truncate Set before test starts ---
    # Note: truncate is asynchronous, so we wait briefly or just use scan/remove
    # for strictly clean state if truncate is slow. For unit tests, scan+remove is safer.
    try:
        scan = aerospike_client.scan(TEST_NAMESPACE, TEST_SET)
        def callback(input_tuple):
            key, _, _ = input_tuple
            aerospike_client.remove(key)
        scan.foreach(callback)
    except Exception:
        pass # Ignore if set is empty
    return store
# def test_basic_put_and_get(store):
#     """Test simple write and read operations."""
#     namespace = ("users", "profiles")
#     key = "user_123"
#     data = {"name": "Alice", "age": 30}
#     # 1. Put Data
#     op = PutOp(namespace=namespace, key=key, value=data)
#     store.batch([op])
#     # 2. Get Data
#     item = store.get(namespace, key)
#     assert item is not None
#     assert item.value == data
#     assert item.key == key
#     assert item.namespace == namespace
#     assert item.created_at is not None
#     assert item.updated_at is not None
# def test_get_missing_item(store):
#     """Test getting a key that doesn't exist."""
#     item = store.get(("ghost", "town"), "non_existent")
#     assert item is None
# def test_batch_operations(store):
#     """Test mixing Put and Get in a batch."""
#     ns = ("memories",)
#     ops = [
#         PutOp(namespace=ns, key="k1", value={"v": 1}),
#         PutOp(namespace=ns, key="k2", value={"v": 2}),
#     ]
#     store.batch(ops)
#     # Now retrieve them in a batch
#     read_ops = [
#         GetOp(namespace=ns, key="k1"),
#         GetOp(namespace=ns, key="k2")
#     ]
#     results = store.batch(read_ops)
#     assert len(results) == 2
#     assert results[0].value == {"v": 1}
#     assert results[1].value == {"v": 2}
# def test_delete_item(store):
#     """Test that putting None deletes the item."""
#     ns = ("temp",)
#     key = "to_delete"
#     # Create
#     store.batch([PutOp(namespace=ns, key=key, value={"data": "here"})])
#     assert store.get(ns, key) is not None
#     # Delete (Put None)
#     store.batch([PutOp(namespace=ns, key=key, value=None)])
#     # Verify
#     assert store.get(ns, key) is None
# def test_search_exact_match(store):
#     """Test searching with filter expressions."""
#     ns = ("documents", "reports")
#     # Setup data
#     ops = [
#         PutOp(namespace=ns, key="doc1", value={"status": "draft", "author": "bob"}),
#         PutOp(namespace=ns, key="doc2", value={"status": "published", "author": "alice"}),
#         PutOp(namespace=ns, key="doc3", value={"status": "draft", "author": "charlie"}),
#     ]
#     store.batch(ops)
#     # Search for status=draft
#     search_op = SearchOp(
#         namespace_prefix=ns,
#         filter={"status": "draft"},
#         limit=10
#     )
#     results = store.batch([search_op])[0]
#     assert len(results) == 2
#     authors = sorted([r.value["author"] for r in results])
#     assert authors == ["bob", "charlie"]
def test_list_namespaces(store):
    """Test listing namespaces with prefixes."""
    # Setup hierarchy
    # ("root", "branch_a", "leaf_1")
    # ("root", "branch_a", "leaf_2")
    # ("root", "branch_b", "leaf_3")
    data = {"dummy": True}
    ops = [
        PutOp(namespace=("root", "branch_a", "leaf_1"), key="k", value=data),
        PutOp(namespace=("root", "branch_a", "leaf_2"), key="k", value=data),
        PutOp(namespace=("root", "branch_b", "leaf_3"), key="k", value=data),
    ]
    store.batch(ops)
    # List with prefix ("root", "branch_a")
    list_op = ListNamespacesOp(
        match_conditions=[MatchCondition(match_type="prefix", path=("root", "branch_a"))],
        max_depth=3
    )
    # Depending on how the user code parses MatchConditions, we might need to verify inputs
    # But based on your implementation:
    results = store.batch([list_op])[0]
    # Should find 2 namespaces: leaf_1 and leaf_2
    assert len(results) == 2
    assert ("root", "branch_a", "leaf_1") in results
    assert ("root", "branch_a", "leaf_2") in results