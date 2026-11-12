"""
Probe 2 for the 'doc4' failure: does updating the graph's ENTRY POINT (the first
object inserted) lose objects, alone or alongside inserts? Plus an exact replay
of the point-2 test sequence that lost doc4.

After every batch it searches for EVERY object in the collection with its own
vector (not just the ones the batch touched), and checks the seeds once before
the batch so a loss can be pinned on the batch. Collections hold <= 6 objects,
so anything reachable must come back in a top-10 search.

    ENDEE_TOKEN=<db token> [ENDEE_URL=...] [PROBE_ROUNDS=20] python orphan_probe2.py
"""
import os
import random
from collections import Counter

from endee import Endee

ROUNDS = int(os.environ.get("PROBE_ROUNDS", "20"))
COLL = "orphan_probe2"

c = Endee(token=os.environ["ENDEE_TOKEN"])
c.set_base_url("http://172.31.11.45:8080/api/v2")


def fresh(fields):
    try:
        c.delete_collection(COLL)
    except Exception:  # noqa: BLE001
        pass
    c.create_collection(COLL, fields=fields)
    return c.get_collection(COLL)


def raw_hits(col, field, vec):
    res = col.search(fields={field: {"query": vec, "limit": 10}})["results"].get(field, [])
    return [(h["id"], round(h["similarity"], 3)) for h in res]


def lost(col, live):
    """live: {field: {id: vec}} -> list of (field, id, raw hits) not found."""
    out = []
    for field, objs in live.items():
        for oid, vec in objs.items():
            hs = raw_hits(col, field, vec)
            if oid not in [h[0] for h in hs]:
                out.append((field, oid, hs))
    return out


def vecfield(name, dim):
    return {"name": name, "type": "vector",
            "params": {"dimension": dim, "space_type": "cosine", "precision": "float32"}}


stats = {}


def record(name, pre, post):
    s = stats.setdefault(name, {"batches": 0, "seed_lost": 0, "lost": 0,
                                "who": Counter(), "first": None})
    s["batches"] += 1
    if pre:
        s["seed_lost"] += 1
    if post:
        s["lost"] += 1
        for field, oid, _ in post:
            s["who"][f"{field}:{oid}"] += 1
        if s["first"] is None:
            field, oid, hs = post[0]
            s["first"] = (f"missing {[(f, o) for f, o, _ in post]}; "
                          f"search {field} for {oid} returned {hs}")


# ---- random-vector cases: seeds s0, s1, s2 one request each; s0 = first = entry point (usually)
DIM = 16
CASES = {
    "C ins + upd s1":       [("x", True), ("s1", False)],              # v1 control: not the entry point
    "F upd entry alone":    [("s0", False)],
    "G ins + upd entry":    [("x", True), ("s0", False)],
    "H 2 ins + upd entry":  [("x", True), ("z", True), ("s0", False)],
}


def run_random_case(name, spec, r):
    rng = random.Random(f"{r}-{name}")
    rv = lambda: [rng.uniform(-1, 1) for _ in range(DIM)]  # noqa: E731
    col = fresh([vecfield("a", DIM)])
    live = {"a": {}}
    for sid in ("s0", "s1", "s2"):
        v = rv()
        col.upsert([{"id": sid, "fields": {"a": v}}])
        live["a"][sid] = v
    pre = lost(col, live)
    batch = [(oid, rv()) for oid, _ in spec]
    col.upsert([{"id": oid, "fields": {"a": v}} for oid, v in batch])
    for oid, v in batch:
        live["a"][oid] = v
    record(name, pre, lost(col, live))


# ---- exact replay of the point-2 test up to and including the mixed batch
def run_replay(r):
    col = fresh([vecfield("a", 4), vecfield("b", 4),
                 {"name": "mv", "type": "multi_vector",
                  "params": {"dimension": 4, "space_type": "cosine", "precision": "float32",
                             "pooling": "mean"}}])
    live = {"a": {}, "b": {}}

    def up(oid, fields, flt=None):
        o = {"id": oid, "fields": fields}
        if flt:
            o["filter"] = flt
        col.upsert([o])
        for f, v in fields.items():
            if f in live:
                live[f][oid] = v

    up("doc1", {"a": [1, 0, 0, 0]}, {"tag": "x"})
    up("doc1", {"b": [0, 1, 0, 0]}, {"tag": "x"})
    up("doc1", {"mv": [[1, 0, 0, 0], [0, 1, 0, 0]]}, {"tag": "x"})
    up("doc2", {"a": [0, 0, 1, 0]})
    up("doc2", {"b": [0, 0, 1, 0]})
    up("doc3", {"a": [0, 0, 0, 1]})
    up("doc3", {"a": [1, 1, 0, 0]})
    pre = lost(col, live)

    batch = [
        {"id": "doc4", "fields": {"a": [0, 1, 1, 0], "b": [0, 1, 1, 0]}},
        {"id": "doc3", "fields": {"b": [1, 0, 0, 1]}},
        {"id": "doc1", "filter": {"tag": "x"}, "fields": {"a": [1, 0, 0, 1]}},
    ]
    col.upsert(batch)
    for o in batch:
        for f, v in o["fields"].items():
            live[f][o["id"]] = v
    record("R replay doc4 test", pre, lost(col, live))


for r in range(ROUNDS):
    for name, spec in CASES.items():
        run_random_case(name, spec, r)
    run_replay(r)
    print(f"round {r + 1}/{ROUNDS} done")

try:
    c.delete_collection(COLL)
except Exception:  # noqa: BLE001
    pass

print(f"\n{'case':<22}{'batches':>8}{'seed lost':>11}{'batch lost':>12}   lost objects (field:id x count)")
for name, s in stats.items():
    who = ", ".join(f"{k} x{v}" for k, v in s["who"].most_common()) or "-"
    print(f"{name:<22}{s['batches']:>8}{s['seed_lost']:>11}{s['lost']:>12}   {who}")
print("\nfirst failure per case:")
for name, s in stats.items():
    print(f"  {name}: {s['first'] or '-'}")
