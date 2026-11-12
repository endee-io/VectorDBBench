"""
Probe: can an object become unsearchable when a batch inserts and updates in the
same field at the same time? (The 'doc4' failure from the point-2 check.)

Each round, for each case: fresh one-field collection, seed 3 objects one request
at a time, send ONE batch, then search for every object the batch touched using
its own vector. The collection holds <= 5 objects, so anything reachable in the
graph must come back in a top-10 search.

    ENDEE_TOKEN=<db token> [ENDEE_URL=...] [PROBE_ROUNDS=20] python orphan_probe.py
"""
import os
import random

from endee import Endee

DIM = 16
ROUNDS = int(os.environ.get("PROBE_ROUNDS", "20"))
VEC = {"dimension": DIM, "space_type": "cosine", "precision": "float32"}

c = Endee(token=os.environ["ENDEE_TOKEN"])
c.set_base_url("http://172.31.11.45:8080/api/v2")


def rvec(rng):
    return [rng.uniform(-1, 1) for _ in range(DIM)]


def found(col, oid, vec):
    res = col.search(fields={"a": {"query": vec, "limit": 10}})["results"].get("a", [])
    return oid in [h["id"] for h in res]


# case -> list of (object id, is it new?) sent together in one batch
CASES = {
    "A insert alone":      [("x", True)],
    "B update alone":      [("s1", False)],
    "C insert + update":   [("x", True), ("s1", False)],   # the suspect
    "D insert + insert":   [("x", True), ("z", True)],
    "E update + update":   [("s1", False), ("s2", False)],
}

stats = {name: {"batches": 0, "lost": 0, "first": None} for name in CASES}

for r in range(ROUNDS):
    for name, batch_spec in CASES.items():
        rng = random.Random(f"{r}-{name}")
        coll = "orphan_probe"
        try:
            c.delete_collection(coll)
        except Exception:  # noqa: BLE001
            pass
        c.create_collection(coll, fields=[{"name": "a", "type": "vector", "params": VEC}])
        col = c.get_collection(coll)

        for sid in ("s0", "s1", "s2"):                     # seed, one request each
            col.upsert([{"id": sid, "fields": {"a": rvec(rng)}}])

        batch = [(oid, rvec(rng)) for oid, _ in batch_spec]
        col.upsert([{"id": oid, "fields": {"a": v}} for oid, v in batch])

        stats[name]["batches"] += 1
        missing = [oid for oid, v in batch if not found(col, oid, v)]
        if missing:
            stats[name]["lost"] += 1
            if stats[name]["first"] is None:
                stats[name]["first"] = f"round {r}: missing {missing}"
    print(f"round {r + 1}/{ROUNDS} done")

try:
    c.delete_collection("orphan_probe")
except Exception:  # noqa: BLE001
    pass

print(f"\n{'case':<20} {'batches':>8} {'lost an object':>15}   first failure")
for name, s in stats.items():
    print(f"{name:<20} {s['batches']:>8} {s['lost']:>15}   {s['first'] or '-'}")
