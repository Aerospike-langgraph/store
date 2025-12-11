import aerospike
import time
import sys

# ==========================================
# CONFIGURATION
# ==========================================
config = {
    'hosts': [('127.0.0.1', 3000)]
}

# Force a touch on every read
ALWAYS_TOUCH_POLICY = {'read_touch_ttl_percent': 100}

try:
    client = aerospike.client(config).connect()
except Exception as e:
    print(f"Failed to connect: {e}")
    sys.exit(1)

# ==========================================
# THE TEST
# ==========================================
print("\n--- STARTING MULTI-RECORD TTL TEST ---\n")

# Define two different keys
key_short = ('test', 'demo', 'short_lived') # Intended TTL: 20s
key_long  = ('test', 'demo', 'long_lived')  # Intended TTL: 60s

# 1. Write records with DIFFERENT TTLs
print("1. Writing Record A (TTL=20s) and Record B (TTL=60s)...")
client.put(key_short, {'name': 'A'}, meta={'ttl': 20})
client.put(key_long,  {'name': 'B'}, meta={'ttl': 60})

# 2. Wait 5 seconds
print("2. Sleeping for 5 seconds...")
print("   (Expectation: A -> ~15s, B -> ~55s)")
time.sleep(5)

# Check status BEFORE the smart read
_, meta_a_before, _ = client.get(key_short)
_, meta_b_before, _ = client.get(key_long)
print(f"   -> Record A TTL before: {meta_a_before['ttl']}s")
print(f"   -> Record B TTL before: {meta_b_before['ttl']}s")

# 3. Perform the Smart Reads
print("\n3. Reading both records with 'read_touch_ttl_percent': 100...")
client.get(key_short, policy=ALWAYS_TOUCH_POLICY)
client.get(key_long,  policy=ALWAYS_TOUCH_POLICY)

# Allow async server update to process
time.sleep(0.1)

# 4. Verify Results
_, meta_a_after, _ = client.get(key_short)
_, meta_b_after, _ = client.get(key_long)

print(f"\n--- RESULTS ---")
print(f"Record A (Target 20s): Current TTL is {meta_a_after['ttl']}s")
print(f"Record B (Target 60s): Current TTL is {meta_b_after['ttl']}s")

# Validation Logic
success_a = meta_a_after['ttl'] >= 19
success_b = meta_b_after['ttl'] >= 59

if success_a and success_b:
    print("\nSUCCESS: Both records reset to their OWN original TTLs!")
else:
    print("\nFAIL: One or both records did not reset correctly.")

client.close()