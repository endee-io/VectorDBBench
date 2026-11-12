"""
Does re-upserting an existing object leave its OLD filter values in the filter index?

addObjects (object.hpp) overwrites the object's stored filter and adds the new values to the
filter index, but never removes the old ones. updateObjectFilter (the filter-update route)
does remove them first. If that is right, an updated object keeps matching filters it no longer
has, and after a delete those stale entries can be inherited by a NEW object that reuses the
freed numeric id.

Also checks the other half of the question: a re-upsert carrying only field `a` must leave
field `b`'s stored vector and graph entry untouched.

Collection up_probe: two dense fields a, b (dim 4, cosine), 20 background objects (grp=bg, n=0)
plus doc1. Every filtered search is on field `a` with limit 30; the matches are few, so they go
to prefilter brute force and every match comes back regardless of the query vector.

    ENDEE_TOKEN=<db token> [ENDEE_URL=...] python upsert_filter_probe.py
"""
import os

import numpy as np
from endee import Endee

COLL = "up_probe"
c = Endee(token=os.environ["ENDEE_TOKEN"])
c.set_base_url("http://172.31.11.45:8080/api/v2")
rng = np.random.default_rng(11)


def vec():
    v = rng.standard_normal(4)
    return (v / np.linalg.norm(v)).tolist()


try:
    c.delete_collection(COLL)
except Exception:  # noqa: BLE001
    pass
c.create_collection(COLL, fields=[
    {"name": "a", "type": "vector", "params": {"space_type": "cosine", "precision": "float32", "dimension": 4}},
    {"name": "b", "type": "vector", "params": {"space_type": "cosine", "precision": "float32", "dimension": 4}},
])
col = c.get_collection(COLL)
col.upsert([{"id": f"bg{i}", "filter": {"grp": "bg", "n": 0}, "fields": {"a": vec(), "b": vec()}}
            for i in range(20)])
A1, B1 = vec(), vec()
col.upsert([{"id": "doc1", "meta": {"v": 1}, "filter": {"grp": "a", "n": 5},
             "fields": {"a": A1, "b": B1}}])

rows = []


def matches(flt):
    res = col.search(fields={"a": {"query": vec(), "limit": 30}}, filter=flt)["results"]["a"]
    return sorted(h["id"] for h in res if not h["id"].startswith("bg"))


def check(step, label, got, want):
    rows.append((step, label, str(want), str(got), "ok" if got == want else "WRONG"))


def stored(oid):
    objs = col.get_objects([oid])
    return objs[0] if objs else None


# 0. as inserted
check(0, "filter grp=a", matches([{"grp": {"$eq": "a"}}]), ["doc1"])
check(0, "filter n=5", matches([{"n": {"$eq": 5}}]), ["doc1"])

# 1. re-upsert doc1 with field a only, filter grp=b n=7
A2 = vec()
col.upsert([{"id": "doc1", "meta": {"v": 2}, "filter": {"grp": "b", "n": 7}, "fields": {"a": A2}}])
o = stored("doc1")
check(1, "stored filter", o["filter"], {"grp": "b", "n": 7})
check(1, "stored meta", o["meta"], {"v": 2})
check(1, "field b vector kept", bool(np.allclose(o["vectors"].get("b", [0] * 4), B1, atol=1e-5)), True)
top_b = col.search(fields={"b": {"query": B1, "limit": 1}})["results"]["b"]
check(1, "field b still finds doc1", [h["id"] for h in top_b], ["doc1"])
check(1, "filter grp=b (new)", matches([{"grp": {"$eq": "b"}}]), ["doc1"])
check(1, "filter n=7 (new)", matches([{"n": {"$eq": 7}}]), ["doc1"])
check(1, "filter grp=a (old)", matches([{"grp": {"$eq": "a"}}]), [])
check(1, "filter n=5 (old)", matches([{"n": {"$eq": 5}}]), [])

# 2. re-upsert doc1 with field a only, no filter and no meta
col.upsert([{"id": "doc1", "fields": {"a": vec()}}])
o = stored("doc1")
check(2, "stored filter", o["filter"], {})
check(2, "filter grp=b (dropped)", matches([{"grp": {"$eq": "b"}}]), [])
check(2, "filter grp=a (older)", matches([{"grp": {"$eq": "a"}}]), [])

# 3. delete doc1, then insert a brand-new object doc3 with grp=c (it may reuse doc1's id)
col.delete_object("doc1")
col.upsert([{"id": "doc3", "filter": {"grp": "c", "n": 9}, "fields": {"a": vec(), "b": vec()}}])
check(3, "filter grp=c (doc3's own)", matches([{"grp": {"$eq": "c"}}]), ["doc3"])
check(3, "filter grp=a (doc1's)", matches([{"grp": {"$eq": "a"}}]), [])
check(3, "filter grp=b (doc1's)", matches([{"grp": {"$eq": "b"}}]), [])
check(3, "filter n=5 (doc1's)", matches([{"n": {"$eq": 5}}]), [])

print(f"{'step':<5}{'check':<28}{'correct':<24}{'got':<24}")
for step, label, want, got, verdict in rows:
    print(f"{step:<5}{label:<28}{want:<24}{got:<24}{verdict}")
print("\nsteps: 0 inserted doc1 (grp=a n=5) | 1 re-upsert a only, grp=b n=7 | "
      "2 re-upsert a only, no filter | 3 delete doc1, insert doc3 (grp=c)")
