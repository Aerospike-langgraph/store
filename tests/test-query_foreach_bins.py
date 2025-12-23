"""
Test cases to verify that Aerospike query.foreach() returns bins correctly
with filter expressions and operations.
"""

import pytest
import aerospike
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import operations as op

# Configuration for local Docker instance
AEROSPIKE_CONFIG = {'hosts': [('localhost', 3000)]}
TEST_NAMESPACE = "test"
TEST_SET = "query_foreach_test"


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
def setup_test_data(aerospike_client):
    """Sets up test data directly using Aerospike client."""
    # Cleanup before test
    try:
        scan = aerospike_client.scan(TEST_NAMESPACE, TEST_SET)
        def callback(input_tuple):
            key, _, _ = input_tuple
            aerospike_client.remove(key)
        scan.foreach(callback)
    except Exception:
        pass  # Ignore if set is empty
    
    # Add some dummy data directly using Aerospike client
    test_data = [
        (("test", "docs"), "doc1", {"text": "machine learning", "category": "tech", "status": "published"}),
        (("test", "docs"), "doc2", {"text": "python programming", "category": "tech", "status": "draft"}),
        (("test", "docs"), "doc3", {"text": "cooking recipes", "category": "food", "status": "published"}),
        (("test", "other"), "doc4", {"text": "travel guide", "category": "travel", "status": "published"}),
    ]
    
    for namespace, key, value in test_data:
        # Create key: (namespace, set, key_string)
        p_key = (TEST_NAMESPACE, TEST_SET, "|".join([*namespace, key]))
        
        # Store bins
        bins = {
            "namespace": list(namespace),
            "key": key,
            "value": value,
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00"
        }
        aerospike_client.put(p_key, bins)
    
    yield
    
    # Cleanup after test (optional)
    # try:
    #     scan = aerospike_client.scan(TEST_NAMESPACE, TEST_SET)
    #     def callback(input_tuple):
    #         key, _, _ = input_tuple
    #         aerospike_client.remove(key)
    #     scan.foreach(callback)
    # except Exception:
    #     pass


def test_query_foreach_with_filter_expressions_only(aerospike_client, setup_test_data):
    """Test query.foreach() with only filter expressions - verify bins are returned."""
    query_obj = aerospike_client.query(TEST_NAMESPACE, TEST_SET)
    
    # Build filter expression to match namespace starting with ("test", "docs")
    namespace_filter = exp.And(
        exp.GE(exp.ListSize(None, exp.ListBin("namespace")), exp.Val(2)),
        exp.Eq(
            exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.STRING, exp.Val(0), exp.ListBin("namespace")),
            exp.Val("test")
        ),
        exp.Eq(
            exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.STRING, exp.Val(1), exp.ListBin("namespace")),
            exp.Val("docs")
        )
    )
    
    policy = {
        "expressions": namespace_filter.compile()
    }
    
    # Collect results
    results = []
    bins_received = []
    
    def collect_callback(record):
        key, meta, bins = record
        results.append((key, bins))
        
        # Check what bins we received
        if bins:
            bins_received.append(list(bins.keys()))
        else:
            bins_received.append(None)
        
        return True
    
    query_obj.foreach(collect_callback, policy)
    
    # Verify we got results
    assert len(results) > 0, "Query should return at least one result"
    
    # Verify all results have bins
    for key, bins in results:
        assert bins is not None, f"Bins should not be None for key {key}"
        assert isinstance(bins, dict), f"Bins should be a dict for key {key}, got {type(bins)}"
        assert len(bins) > 0, f"Bins should not be empty for key {key}"
    
    # Verify specific bins exist
    for key, bins in results:
        assert "namespace" in bins, f"'namespace' bin should be present for key {key}"
        assert "key" in bins, f"'key' bin should be present for key {key}"
        assert "value" in bins, f"'value' bin should be present for key {key}"
    
    print(f"\n=== Test 1: Filter expressions only ===")
    print(f"Results count: {len(results)}")
    print(f"Bins received: {bins_received}")
    for key, bins in results:
        print(f"  Key: {key}, Bins: {list(bins.keys())}")


def test_query_foreach_with_add_ops_and_filter_expressions(aerospike_client, setup_test_data):
    """Test query.foreach() with add_ops and filter expressions - verify bins are returned."""
    query_obj = aerospike_client.query(TEST_NAMESPACE, TEST_SET)
    
    # Add read operations
    query_obj.add_ops([
        op.read("namespace"),
        op.read("key"),
        op.read("value"),
        op.read("created_at"),
        op.read("updated_at")
    ])
    
    # Build filter expression to match namespace starting with ("test", "docs")
    namespace_filter = exp.And(
        exp.GE(exp.ListSize(None, exp.ListBin("namespace")), exp.Val(2)),
        exp.Eq(
            exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.STRING, exp.Val(0), exp.ListBin("namespace")),
            exp.Val("test")
        ),
        exp.Eq(
            exp.ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, exp.ResultType.STRING, exp.Val(1), exp.ListBin("namespace")),
            exp.Val("docs")
        )
    )
    
    policy = {
        "expressions": namespace_filter.compile()
    }
    
    # Collect results
    results = []
    bins_received = []
    
    def collect_callback(record):
        key, meta, bins = record
        results.append((key, bins))
        
        # Check what bins we received
        if bins:
            bins_received.append(list(bins.keys()))
        else:
            bins_received.append(None)
        
        return True
    
    query_obj.foreach(collect_callback, policy)
    
    # Verify we got results
    assert len(results) > 0, "Query should return at least one result"
    
    # Verify all results have bins
    for key, bins in results:
        assert bins is not None, f"Bins should not be None for key {key}"
        assert isinstance(bins, dict), f"Bins should be a dict for key {key}, got {type(bins)}"
        assert len(bins) > 0, f"Bins should not be empty for key {key}"
    
    # Verify specific bins exist (the ones we requested via read ops)
    for key, bins in results:
        assert "namespace" in bins, f"'namespace' bin should be present for key {key}"
        assert "key" in bins, f"'key' bin should be present for key {key}"
        assert "value" in bins, f"'value' bin should be present for key {key}"
        assert "created_at" in bins, f"'created_at' bin should be present for key {key}"
        assert "updated_at" in bins, f"'updated_at' bin should be present for key {key}"
    
    print(f"\n=== Test 2: add_ops + filter expressions ===")
    print(f"Results count: {len(results)}")
    print(f"Bins received: {bins_received}")
    for key, bins in results:
        print(f"  Key: {key}, Bins: {list(bins.keys())}")


def test_query_foreach_without_filter_expressions(aerospike_client, setup_test_data):
    """Test query.foreach() without filter expressions - verify bins are returned."""
    query_obj = aerospike_client.query(TEST_NAMESPACE, TEST_SET)
    
    # Add read operations
    query_obj.add_ops([
        op.read("namespace"),
        op.read("key"),
        op.read("value")
    ])
    
    # No filter expressions - should return all records
    
    # Collect results
    results = []
    bins_received = []
    
    def collect_callback(record):
        key, meta, bins = record
        results.append((key, bins))
        
        # Check what bins we received
        if bins:
            bins_received.append(list(bins.keys()))
        else:
            bins_received.append(None)
        
        return True
    
    query_obj.foreach(collect_callback)
    
    # Verify we got results
    assert len(results) > 0, "Query should return at least one result"
    
    # Verify all results have bins
    for key, bins in results:
        assert bins is not None, f"Bins should not be None for key {key}"
        assert isinstance(bins, dict), f"Bins should be a dict for key {key}, got {type(bins)}"
        assert len(bins) > 0, f"Bins should not be empty for key {key}"
    
    print(f"\n=== Test 3: add_ops only (no filter) ===")
    print(f"Results count: {len(results)}")
    print(f"Bins received: {bins_received}")
    for key, bins in results:
        print(f"  Key: {key}, Bins: {list(bins.keys())}")


def test_query_foreach_with_value_filter(aerospike_client, setup_test_data):
    """Test query.foreach() with value-based filter expressions."""
    query_obj = aerospike_client.query(TEST_NAMESPACE, TEST_SET)
    
    # Add read operations
    query_obj.add_ops([
        op.read("namespace"),
        op.read("key"),
        op.read("value"),
        op.read("created_at"),
        op.read("updated_at")
    ])
    
    # Filter by value.category == "tech"
    category_filter = exp.Eq(
        exp.MapGetByKey(
            None,
            aerospike.MAP_RETURN_VALUE,
            exp.ResultType.STRING,
            exp.Val("category"),
            exp.MapBin("value")
        ),
        exp.Val("tech")
    )
    
    policy = {
        "expressions": category_filter.compile()
    }
    
    # Collect results
    results = []
    bins_received = []
    
    def collect_callback(record):
        key, meta, bins = record
        results.append((key, bins))
        
        # Check what bins we received
        if bins:
            bins_received.append(list(bins.keys()))
        else:
            bins_received.append(None)
        
        return True
    
    query_obj.foreach(collect_callback, policy)
    
    # Verify we got results
    assert len(results) > 0, "Query should return at least one result"
    
    # Verify all results have bins
    for key, bins in results:
        assert bins is not None, f"Bins should not be None for key {key}"
        assert isinstance(bins, dict), f"Bins should be a dict for key {key}, got {type(bins)}"
        assert len(bins) > 0, f"Bins should not be empty for key {key}"
        
        # Verify the value bin contains the expected category
        if "value" in bins and isinstance(bins["value"], dict):
            assert bins["value"].get("category") == "tech", \
                f"Filtered result should have category='tech', got {bins['value'].get('category')}"
    
    print(f"\n=== Test 4: Value filter (category='tech') ===")
    print(f"Results count: {len(results)}")
    print(f"Bins received: {bins_received}")
    for key, bins in results:
        print(f"  Key: {key}, Bins: {list(bins.keys())}")
        if "value" in bins:
            print(f"    Value: {bins['value']}")