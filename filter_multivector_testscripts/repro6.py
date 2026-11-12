"""
Mentor point 4: filtered search must return the SAME results before and after the change
(postfilter now gets the raw filter; prefilter still gets filter AND presence).

Collection p4_compare, 20,000 objects:
  a  : dense, every object            (full coverage)
  b  : dense, ~30% of objects         (partial coverage -> presence matters)
  mv : multi-vector, ~40% of objects  (partial coverage, two-stage search)
  filter grp: g0 ~70%, g1 ~25%, g2 ~4.5%, g3 ~0.5%
Each (field, filter, prefilter_threshold) is searched with 10 fixed queries; the expected
path (pre/post) is printed so you can see both paths are covered.

The graph must be the SAME for both runs (HNSW construction is randomized), so:
  1. old image:  python point4_compare.py --setup
  2. old image:  python point4_compare.py --run before.json
  3. swap to the new image (docker restart loads the saved graph; do NOT run --setup again)
  4. new image:  python point4_compare.py --run after.json
  5.             python point4_compare.py --diff before.json after.json

    ENDEE_TOKEN=<db token> [ENDEE_URL=http://host:8080/api/v2] python point4_compare.py ...
"""
import json
import os
import random
import sys

import msgpack
import requests

COLL = "p4_compare"
N = 20_000
DA, DMV = 32, 16
DEFAULT_THRESHOLD = 10_000
THRESHOLDS = [None, 1_000, 1_000_000]          # None = server default (10,000)
FILTERS = {"none": None, "g0": "g0", "g1": "g1", "g3": "g3"}
QUERIES = 10


def dataset():
    """Deterministic: same objects (and field coverage) every call."""
    rng = random.Random(42)
    objs = []
    for i in range(N):
        r = rng.random()
        grp = "g0" if r < 0.70 else "g1" if r < 0.95 else "g2" if r < 0.995 else "g3"
        o = {"id": f"o{i}", "filter": {"grp": grp},
             "fields": {"a": [rng.uniform(-1, 1) for _ in range(DA)]}}
        if rng.random() < 0.30:
            o["fields"]["b"] = [rng.uniform(-1, 1) for _ in range(DA)]
        if rng.random() < 0.40:
            o["fields"]["mv"] = [[rng.uniform(-1, 1) for _ in range(DMV)]
                                 for _ in range(rng.randint(2, 5))]
        objs.append(o)
    return objs


def queries(field):
    rng = random.Random(f"q-{field}")
    if field == "mv":
        return [[[rng.uniform(-1, 1) for _ in range(DMV)] for _ in range(3)] for _ in range(QUERIES)]
    return [[rng.uniform(-1, 1) for _ in range(DA)] for _ in range(QUERIES)]


TOKEN="prefilter:wKuwPWQT00E8rVGvGXCrNE6HQ5c14QRi"
BASE="http://172.31.11.45:8080/api/v2"
S = requests.Session()
S.headers["Authorization"] = TOKEN


def search(field, q, grp, threshold):
    body = {"fields": [{field: {"query": q, "limit": 10}}]}
    if grp is not None:
        body["filter"] = [{"grp": {"$eq": grp}}]
    if threshold is not None:
        body["filter_params"] = {"prefilter_threshold": threshold}
    r = S.post(f"{BASE}/collection/{COLL}/search", json=body, timeout=120)
    if r.status_code != 200:
        return {"error": f"{r.status_code} {r.text[:200]}"}
    raw = msgpack.unpackb(r.content, raw=False, strict_map_key=False)
    objects, results = raw[0], raw[1]
    return [[objects.get(h[0], [str(h[0])])[0], round(float(h[1]), 5)] for h in results.get(field, [])]


def setup():
    from endee import Endee
    c = Endee(token="prefilter:wKuwPWQT00E8rVGvGXCrNE6HQ5c14QRi")
    c.set_base_url("http://172.31.11.45:8080/api/v2")
    try:
        c.delete_collection(COLL)
    except Exception:  # noqa: BLE001
        pass
    vec = {"space_type": "cosine", "precision": "float32"}
    c.create_collection(COLL, fields=[
        {"name": "a", "type": "vector", "params": dict(vec, dimension=DA)},
        {"name": "b", "type": "vector", "params": dict(vec, dimension=DA)},
        {"name": "mv", "type": "multi_vector", "params": dict(vec, dimension=DMV, pooling="mean")},
    ])
    col = c.get_collection(COLL)
    objs = dataset()
    for i in range(0, N, 1000):          # inserts only: no updates (avoids the slot-0 bug)
        col.upsert(objs[i:i + 1000])
        print(f"  inserted {min(i + 1000, N)}/{N}")
    print("setup done. Now: docker restart endee  (graceful: saves the graph), then --run before.json")


def run(out):
    objs = dataset()
    counts = {}  # field-aware counts: (field, grp) -> n
    for o in objs:
        for f in ("a", "b", "mv"):
            if f in o["fields"]:
                counts[(f, o["filter"]["grp"])] = counts.get((f, o["filter"]["grp"]), 0) + 1
    res = {}
    print(f"{'field':<4}{'filter':<7}{'threshold':>11}{'count':>8}  path")
    for field in ("a", "b", "mv"):
        qs = queries(field)
        for fname, grp in FILTERS.items():
            for t in (THRESHOLDS if grp else [None]):
                n = counts.get((field, grp), 0) if grp else None
                eff = DEFAULT_THRESHOLD if t is None else t
                path = "no filter" if grp is None else ("postfilter" if n >= eff else "prefilter")
                print(f"{field:<4}{fname:<7}{str(t or 'default'):>11}{str(n if n is not None else '-'):>8}  {path}")
                res[f"{field}|{fname}|{t}"] = {"path": path,
                                               "results": [search(field, q, grp, t) for q in qs]}
    with open(out, "w") as f:
        json.dump(res, f)
    errors = sum(1 for v in res.values() for r in v["results"] if isinstance(r, dict))
    print(f"\nsaved {len(res)} combos x {QUERIES} queries to {out}; errors: {errors}")


def diff(a_path, b_path):
    a, b = json.load(open(a_path)), json.load(open(b_path))
    exact = tie_order = different = 0
    for key in a:
        for qi, (ra, rb) in enumerate(zip(a[key]["results"], b[key]["results"])):
            if ra == rb:
                exact += 1
            elif (isinstance(ra, list) and isinstance(rb, list)
                  and sorted(ra, key=lambda h: (-h[1], h[0])) == sorted(rb, key=lambda h: (-h[1], h[0]))):
                tie_order += 1
            else:
                different += 1
                print(f"DIFFERENT {key} [{a[key]['path']}] query {qi}:\n  before {ra}\n  after  {rb}")
    total = exact + tie_order + different
    print(f"\n{total} searches: {exact} identical, {tie_order} same hits/scores but tie order differs, "
          f"{different} DIFFERENT")


if "--setup" in sys.argv:
    setup()
elif "--run" in sys.argv:
    run(sys.argv[sys.argv.index("--run") + 1])
elif "--diff" in sys.argv:
    i = sys.argv.index("--diff")
    diff(sys.argv[i + 1], sys.argv[i + 2])
else:
    print(__doc__)
