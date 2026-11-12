"""
Reproduction for mentor point 2: adding a NEW field to an EXISTING object
fails the graph insert, returns 500, and leaves the vector in storage but
absent from that field's HNSW.

Run:  ENDEE_TOKEN="root/<your_db>:$NDD_ROOT_TOKEN" python3 repro_point2.py
      (root alone is blocked from data ops; impersonate a db, or use a db token)
"""
import os
import sys

from endee import Endee

COLL = "repro_point2"
DIM = 4

client = Endee(token=os.environ["ENDEE_TOKEN"])
client.set_base_url("http://172.31.11.45:8080/api/v2")

# Clean slate
try:
    client.delete_collection(COLL)
except Exception:
    pass

client.create_collection(
    COLL,
    fields=[
        {"name": "a", "type": "vector",
         "params": {"dimension": DIM, "space_type": "cosine", "precision": "float32"}},
        {"name": "b", "type": "vector",
         "params": {"dimension": DIM, "space_type": "cosine", "precision": "float32"}},
    ],
)
col = client.get_collection(COLL)

# --- Step 1: create doc1 with ONLY field "a". is_new=True -> addPoint<true>. OK.
print("step 1: upsert doc1 with field 'a' only")
col.upsert([{"id": "doc1", "fields": {"a": [1.0, 0.0, 0.0, 0.0]}}])
print("        -> ok\n")

# --- Step 2: add field "b" to the SAME object. The object-level is_new is now
#     False, so field b's pending entry gets is_new=False -> addPoint<false>
#     -> labelLookup_[id] == INVALID_ID on b's graph -> code 100 -> HTTP 500.
print("step 2: upsert doc1 with field 'b' (field it never had)")
failed = False
try:
    col.upsert([{"id": "doc1", "fields": {"b": [0.0, 1.0, 0.0, 0.0]}}])
    print("        -> NO ERROR (bug did not reproduce)\n")
except Exception as e:
    failed = True
    print(f"        -> {type(e).__name__}: {e}\n")

# --- Step 3: the vector IS in storage. Proves storage/graph divergence.
print("step 3: fetch doc1 — is the 'b' vector stored?")
objs = col.get_objects(["doc1"])
fields = (objs[0] or {}).get("vectors", {}) if objs else {}
print(f"        -> vector fields stored: {sorted(fields.keys())}")
print(f"        -> 'b' in storage: {'b' in fields}\n")

# --- Step 4: but it is NOT searchable on field b.
print("step 4: search field 'b' for its own vector")
res = col.search(fields={"b": {"query": [0.0, 1.0, 0.0, 0.0], "limit": 10}})
hits = res.get("results", {}).get("b", [])
print(f"        -> raw hits: {hits}")
print(f"        -> doc1 found: {any(h.get('id') == 'doc1' for h in hits)}\n")

print("=" * 60)
print("Expected if the bug is present:")
print("  step 2 -> 500 'addPoint: cannot update label N: not present in the graph'")
print("  step 3 -> 'b' in storage: True")
print("  step 4 -> doc1 found: False")
sys.exit(0 if failed else 1)
