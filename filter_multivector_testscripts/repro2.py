"""
Mentor point 2 check: new field on an existing object, plus regressions.
Runs against an ALREADY-RUNNING server. Uses its own collection 'p2_check'.

Full run (recreates p2_check):
    ENDEE_TOKEN=<db token> [ENDEE_URL=http://host:8080/api/v2] python point2_check.py
After restarting the server, re-check what survived the save/reload:
    ENDEE_TOKEN=... [ENDEE_URL=...] python point2_check.py --after-restart
"""
import json
import os
import sys

import requests
from endee import Endee

COLL = "p2_check"
TAGX = [{"tag": {"$eq": "x"}}]
VEC = {"dimension": 4, "space_type": "cosine", "precision": "float32"}
AFTER_RESTART = "--after-restart" in sys.argv

c = Endee(token=os.environ["ENDEE_TOKEN"])
c.set_base_url("http://172.31.11.45:8080/api/v2")

results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -- {detail}" if detail else ""))


def hits(col, field, query, flt=None, limit=10):
    kw = {"fields": {field: {"query": query, "limit": limit}}}
    if flt is not None:
        kw["filter"] = flt
    return col.search(**kw)["results"].get(field, [])


def ids(hs):
    return [h["id"] for h in hs]


def upsert(col, objs):
    try:
        col.upsert(objs)
        return True, ""
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {str(e)[:160]}"


def raw_json_upsert(objs):
    # The Python client rejects duplicate ids, so post JSON directly.
    r = requests.post(f"{c.base_url}/collection/{COLL}/objects",
                      headers={"Authorization": c.token, "Content-Type": "application/json"},
                      data=json.dumps(objs), timeout=30)
    return r.status_code, r.text[:200]


def after_restart_checks(col):
    print("S6  after restart (graph saved + reloaded)")
    check("doc1 on b", "doc1" in ids(hits(col, "b", [0, 1, 0, 0])))
    check("doc1 on b FILTERED (presence rebuilt at load)", "doc1" in ids(hits(col, "b", [0, 1, 0, 0], TAGX)))
    check("doc1 on mv", "doc1" in ids(hits(col, "mv", [[1, 0, 0, 0]])))
    hb = ids(hits(col, "b", [0, 0, 1, 0]))
    check("doc2 exactly once on b", hb.count("doc2") == 1, str(hb))
    check("doc4 on b, not on a",
          "doc4" in ids(hits(col, "b", [0, 1, 1, 0])) and "doc4" not in ids(hits(col, "a", [0, 1, 1, 0])))


if AFTER_RESTART:
    after_restart_checks(c.get_collection(COLL))
    print(f"\n{sum(results)}/{len(results)} passed")
    sys.exit(0 if all(results) else 1)

try:
    c.delete_collection(COLL)
except Exception:  # noqa: BLE001
    pass
c.create_collection(COLL, fields=[
    {"name": "a", "type": "vector", "params": VEC},
    {"name": "b", "type": "vector", "params": VEC},
    {"name": "mv", "type": "multi_vector", "params": dict(VEC, pooling="mean")},
])
col = c.get_collection(COLL)

print("S1  existing object gains a new dense field  (the reported bug)")
ok, _ = upsert(col, [{"id": "doc1", "filter": {"tag": "x"}, "fields": {"a": [1, 0, 0, 0]}}])
check("create doc1 with only 'a'", ok)
ok, err = upsert(col, [{"id": "doc1", "filter": {"tag": "x"}, "fields": {"b": [0, 1, 0, 0]}}])
check("add 'b' to doc1 returns 200", ok, err)
stored = col.get_objects(["doc1"])[0]["vectors"]
check("doc1 stores a+b (merge)", sorted(stored) == ["a", "b"], str(sorted(stored)))
check("search b finds doc1", "doc1" in ids(hits(col, "b", [0, 1, 0, 0])))
check("FILTERED search b finds doc1 (presence updated)", "doc1" in ids(hits(col, "b", [0, 1, 0, 0], TAGX)))
check("search a still finds doc1", "doc1" in ids(hits(col, "a", [1, 0, 0, 0])))

print("S2  existing object gains a multi-vector field (pooled graph)")
ok, err = upsert(col, [{"id": "doc1", "filter": {"tag": "x"},
                        "fields": {"mv": [[1, 0, 0, 0], [0, 1, 0, 0]]}}])
check("add 'mv' to doc1 returns 200", ok, err)
check("search mv finds doc1", "doc1" in ids(hits(col, "mv", [[1, 0, 0, 0]])))
check("FILTERED search mv finds doc1", "doc1" in ids(hits(col, "mv", [[1, 0, 0, 0]], TAGX)))

print("S3  same existing id twice in ONE batch, gaining a new field")
ok, _ = upsert(col, [{"id": "doc2", "fields": {"a": [0, 0, 1, 0]}}])
check("create doc2 with only 'a'", ok)
code, body = raw_json_upsert([{"id": "doc2", "vectors": {"b": [0, 1, 0, 0]}},
                              {"id": "doc2", "vectors": {"b": [0, 0, 1, 0]}}])
check("duplicate-id batch returns 200", code == 200, f"{code} {body}")
hb = ids(hits(col, "b", [0, 0, 1, 0]))
check("doc2 exactly once on b (no orphan slot)", hb.count("doc2") == 1, str(hb))
sb = col.get_objects(["doc2"])[0]["vectors"].get("b")
check("stored b is the LAST write", sb is not None and abs(sb[2] - 1.0) < 1e-3, str(sb))
top = hits(col, "b", [0, 0, 1, 0], limit=1)
check("graph matches storage (top hit doc2, sim~1)",
      bool(top) and top[0]["id"] == "doc2" and top[0]["similarity"] > 0.99, str(top))

print("S4  regression: plain insert, plain update, mixed batch")
ok, _ = upsert(col, [{"id": "doc3", "fields": {"a": [0, 0, 0, 1]}}])
check("new doc3 with only 'a'", ok)
check("doc3 is top hit on a", ids(hits(col, "a", [0, 0, 0, 1], limit=1)) == ["doc3"])
check("doc3 NOT on b", "doc3" not in ids(hits(col, "b", [0, 0, 0, 1])))
ok, _ = upsert(col, [{"id": "doc3", "fields": {"a": [1, 1, 0, 0]}}])
check("update doc3's existing 'a'", ok)
t = hits(col, "a", [1, 1, 0, 0], limit=1)
check("updated doc3 is top hit on a", bool(t) and t[0]["id"] == "doc3" and t[0]["similarity"] > 0.99, str(t))
ok, err = upsert(col, [
    {"id": "doc4", "fields": {"a": [0, 1, 1, 0], "b": [0, 1, 1, 0]}},           # new, two fields
    {"id": "doc3", "fields": {"b": [1, 0, 0, 1]}},                              # existing, gains b
    {"id": "doc1", "filter": {"tag": "x"}, "fields": {"a": [1, 0, 0, 1]}},      # existing, updates a
])
check("mixed batch returns 200", ok, err)
check("doc4 on a and b",
      "doc4" in ids(hits(col, "a", [0, 1, 1, 0])) and "doc4" in ids(hits(col, "b", [0, 1, 1, 0])))
check("doc3 now on b", "doc3" in ids(hits(col, "b", [1, 0, 0, 1])))
t = hits(col, "a", [1, 0, 0, 1], limit=1)
check("doc1's updated a is top hit", bool(t) and t[0]["id"] == "doc1" and t[0]["similarity"] > 0.99, str(t))

print("S5  delete, then re-add with a different field set (recycled id)")
col.delete_object("doc4")
check("doc4 gone from a", "doc4" not in ids(hits(col, "a", [0, 1, 1, 0])))
check("doc4 gone from b", "doc4" not in ids(hits(col, "b", [0, 1, 1, 0])))
ok, err = upsert(col, [{"id": "doc4", "fields": {"b": [0, 1, 1, 0]}}])
check("re-add doc4 with only 'b'", ok, err)
check("doc4 back on b", "doc4" in ids(hits(col, "b", [0, 1, 1, 0])))
check("doc4 NOT on a", "doc4" not in ids(hits(col, "a", [0, 1, 1, 0])))

print(f"\n{sum(results)}/{len(results)} passed")
print("Next: restart the server, then run again with --after-restart")
sys.exit(0 if all(results) else 1)
