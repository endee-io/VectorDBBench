"""
Mentor point 3: a failed save during collection load leaves presence empty, so
filtered search returns 0 results while unfiltered search still works.

  1. python point3_check.py --setup     # create p3_check, upsert 5 objects tagged x
  2. (shell) crash the server, block the graph save, start it again - see the steps
  3. python point3_check.py --check     # first request triggers the load

Uses raw HTTP for searches so the client's retry-on-500 can't hide the first response.
    ENDEE_TOKEN=<db token> [ENDEE_URL=http://host:8080/api/v2] python point3_check.py --setup|--check
"""
import os
import sys

import msgpack
import requests
from endee import Endee

COLL = "p3_check"
c = Endee(token=os.environ["ENDEE_TOKEN"])
c.set_base_url("http://172.31.11.45:8080/api/v2")

DOCS = {f"doc{i}": [1.0 if j == i % 4 else 0.1 for j in range(4)] for i in range(1, 6)}
TAGX = [{"tag": {"$eq": "x"}}]


def raw_search(query, flt=None):
    body = {"fields": [{"a": {"query": query, "limit": 10}}]}
    if flt is not None:
        body["filter"] = flt
    r = requests.post(f"{c.base_url}/collection/{COLL}/search",
                      headers={"Authorization": c.token}, json=body, timeout=60)
    if r.status_code != 200:
        return r.status_code, r.text[:200]
    raw = msgpack.unpackb(r.content, raw=False, strict_map_key=False)
    objects, results = raw[0], raw[1]
    ids = [objects.get(h[0], [str(h[0])])[0] for h in results.get("a", [])]
    return 200, ids


if "--setup" in sys.argv:
    try:
        c.delete_collection(COLL)
    except Exception:  # noqa: BLE001
        pass
    c.create_collection(COLL, fields=[{"name": "a", "type": "vector",
                                       "params": {"dimension": 4, "space_type": "cosine",
                                                  "precision": "float32"}}])
    col = c.get_collection(COLL)
    col.upsert([{"id": k, "filter": {"tag": "x"}, "fields": {"a": v}} for k, v in DOCS.items()])
    print("setup done: 5 objects tagged x are in the WAL (not yet saved).")
    print("sanity  filtered:", raw_search(DOCS["doc1"], TAGX))
    print("Now crash the server IMMEDIATELY (before autosave), then block the save and restart.")

elif "--check" in sys.argv:
    q = DOCS["doc1"]
    s1 = raw_search(q, TAGX)
    s2 = raw_search(q, TAGX)
    s3 = raw_search(q)
    print(f"request 1 (triggers load) filtered  : {s1}")
    print(f"request 2                 filtered  : {s2}")
    print(f"request 3                 unfiltered: {s3}")
    bug = s2[0] == 200 and s2[1] == [] and s3[0] == 200 and len(s3[1]) == 5
    print("\nBUG REPRODUCED: filtered empty, unfiltered has all 5" if bug
          else "\nfiltered search returns results" if s2[0] == 200 and len(s2[1]) == 5
          else "\nunexpected - paste this output")
else:
    print(__doc__)
