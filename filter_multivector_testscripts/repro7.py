"""
Mentor point 6: with a ColBERT-shaped query (32 tokens) over 100-token documents, the
multi-vector exact-MaxSim path fits only ~3 documents in its 10,000-dot-product budget, so
nearly every filtered search falls back to the pooled path.

Collection p6_colbert, one multi-vector field `mv` (dim 16, 100 members per document), and
four filter groups:
  s : 100 docs   <= max(ef, limit) = 128 -> the fallback reranks every doc, so it is exact anyway
  m : 300 docs   >  128                  -> fallback keeps only the pooled top-128: lossy
  l : 1000 docs  >  128                  -> lossy (and still over budget after the fix)
  x : 600 docs   never searched (filler)
Every group is below prefilter_threshold (10,000), so all searches are on the prefilter side.

For each group, 10 fixed 32-token queries are sent with limit 10. The ground truth is exact MaxSim over
that group's docs, computed here in numpy. recall@10 = 1.0 means the server returned exactly
the true top 10.

    ENDEE_TOKEN=<db token> [ENDEE_URL=http://host:8080/api/v2] python point6_colbert.py --setup
    ENDEE_TOKEN=... python point6_colbert.py --run      # once on the old image, once on the new
"""
import os
import statistics
import sys
import time

import msgpack
import numpy as np
import requests

COLL = "p6_colbert"
DIM, DOC_TOKENS, Q_TOKENS = 16, 100, 32
GROUPS = {"s": 100, "m": 300, "l": 1000, "x": 600}
QUERIES, LIMIT = 10, 10


def unit(a):
    return (a / np.linalg.norm(a, axis=-1, keepdims=True)).astype(np.float32)


def dataset():
    """Deterministic: identical docs and groups on every call."""
    rng = np.random.default_rng(6)
    docs = []  # (id, group, members[DOC_TOKENS, DIM])
    i = 0
    for g, n in GROUPS.items():
        for _ in range(n):
            docs.append((f"d{i}", g, unit(rng.standard_normal((DOC_TOKENS, DIM)))))
            i += 1
    return docs


def queries():
    rng = np.random.default_rng(66)
    return [unit(rng.standard_normal((Q_TOKENS, DIM))) for _ in range(QUERIES)]


def maxsim(q, doc):
    return float((q @ doc.T).max(axis=1).sum())  # sum_j max_i dot(q_j, v_i)


TOKEN="prefilter:wKuwPWQT00E8rVGvGXCrNE6HQ5c14QRi"
BASE="http://172.31.11.45:8080/api/v2"
if len(TOKEN.split(":")) > 2:  # cloud-style token carries its region, like the client does
    p = TOKEN.split(":")
    BASE, TOKEN = f"https://{p[2]}.endee.io/api/v2", f"{p[0]}:{p[1]}"
S = requests.Session()
S.headers["Authorization"] = TOKEN


def search(q, grp):
    body = {"fields": [{"mv": {"query": q.tolist(), "limit": LIMIT}}],
            "filter": [{"grp": {"$eq": grp}}]}
    t = time.perf_counter()
    r = S.post(f"{BASE}/collection/{COLL}/search", json=body, timeout=120)
    ms = (time.perf_counter() - t) * 1000
    if r.status_code != 200:
        raise SystemExit(f"search failed: {r.status_code} {r.text[:300]}")
    raw = msgpack.unpackb(r.content, raw=False, strict_map_key=False)
    objects, results = raw[0], raw[1]
    return [objects.get(h[0], [str(h[0])])[0] for h in results.get("mv", [])], ms


def setup():
    from endee import Endee
    c = Endee(token="prefilter:wKuwPWQT00E8rVGvGXCrNE6HQ5c14QRi")
    c.set_base_url("http://172.31.11.45:8080/api/v2")
    try:
        c.delete_collection(COLL)
    except Exception:  # noqa: BLE001
        pass
    c.create_collection(COLL, fields=[
        {"name": "mv", "type": "multi_vector",
         "params": {"space_type": "cosine", "precision": "float32", "dimension": DIM,
                    "pooling": "mean"}},
    ])
    col = c.get_collection(COLL)
    docs = dataset()
    for i in range(0, len(docs), 50):
        col.upsert([{"id": d, "filter": {"grp": g}, "fields": {"mv": m.tolist()}}
                    for d, g, m in docs[i:i + 50]])
        print(f"  inserted {min(i + 50, len(docs))}/{len(docs)}")
    print("setup done; now run --run")


def run():
    docs = dataset()
    qs = queries()
    print(f"query {Q_TOKENS} tokens x doc {DOC_TOKENS} tokens = {Q_TOKENS * DOC_TOKENS} dot products "
          f"per doc; budget 10,000 fits {10_000 // (Q_TOKENS * DOC_TOKENS)} docs\n")
    print(f"{'group':<6}{'docs':>6}{'recall@10':>11}{'exact top-1 found':>19}{'median ms':>11}")
    for g in ("s", "m", "l"):
        members = [(d, m) for d, grp, m in docs if grp == g]
        recalls, top1, times = [], 0, []
        for q in qs:
            truth = sorted(members, key=lambda dm: -maxsim(q, dm[1]))[:LIMIT]
            truth_ids = [d for d, _ in truth]
            got, ms = search(q, g)
            recalls.append(len(set(got) & set(truth_ids)) / LIMIT)
            top1 += truth_ids[0] in got
            times.append(ms)
        print(f"{g:<6}{len(members):>6}{statistics.mean(recalls):>11.2f}"
              f"{f'{top1}/{QUERIES}':>19}{statistics.median(times):>11.1f}")


if "--setup" in sys.argv:
    setup()
elif "--run" in sys.argv:
    run()
else:
    print(__doc__)
