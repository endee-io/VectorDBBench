"""
Mentor point 7: the multi-vector exact path holds every fetched record in memory until the
loop ends, instead of scoring each one as it is read.

Collection p7_mem: one multi-vector field `mv` (dim 384, float32, 100 members per doc), and
1,000 docs, all matching the filter grp=g. A 1-token query costs 1,000 x 100 = 100k dot
products. That is within the exact-path budget (10,000 x 128 = 1.28M), so every query takes the
exact path and, on the current code, holds all 100k members (~154 MB) at once.

--run fires single-token queries from 4 threads at once. While they run, it samples the server
process's ANONYMOUS memory (RssAnon: heap, not the MDBX file mapping) every 20 ms. It
reports the peak growth over the idle baseline, the median latency, and recall@10 against an
exact numpy ground truth (should be 1.00 both before and after: the fix must not change results).

Run it on the same host as the server (it reads /proc/<pid>/status of the ndd process).

    ENDEE_TOKEN=<db token> python point7_memory.py --setup        # ~350 MB upload, once
    docker restart endee                                          # clean baseline
    ENDEE_TOKEN=<db token> python point7_memory.py --run          # current image
    (deploy the fixed image, which restarts it)
    ENDEE_TOKEN=<db token> python point7_memory.py --run          # fixed image
"""
import os
import statistics
import sys
import threading
import time

import msgpack
import numpy as np
import requests

COLL = "p7_mem"
DIM, MEMBERS, DOCS = 384, 100, 1000
QUERIES, THREADS, ROUNDS, LIMIT = 8, 4, 5, 10


def unit(a):
    return (a / np.linalg.norm(a, axis=-1, keepdims=True)).astype(np.float32)


def dataset():
    rng = np.random.default_rng(7)
    return unit(rng.standard_normal((DOCS, MEMBERS, DIM)))  # (docs, members, dim)


def queries():
    rng = np.random.default_rng(77)
    return [unit(rng.standard_normal((1, DIM))) for _ in range(QUERIES)]


TOKEN="prefilter:wKuwPWQT00E8rVGvGXCrNE6HQ5c14QRi"
BASE="http://172.31.11.45:8080/api/v2"
if len(TOKEN.split(":")) > 2:  # cloud-style token carries its region, like the client does
    p = TOKEN.split(":")
    BASE, TOKEN = f"https://{p[2]}.endee.io/api/v2", f"{p[0]}:{p[1]}"


def find_pid():
    if "--pid" in sys.argv:
        return int(sys.argv[sys.argv.index("--pid") + 1])
    found = []
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                with open(f"/proc/{d}/comm") as f:
                    if f.read().strip().startswith("ndd"):
                        found.append(int(d))
            except OSError:
                pass
    if len(found) != 1:
        return None  # server on another host: measure memory there with memwatch.py
    return found[0]


def rss_anon_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("RssAnon:"):
                return int(line.split()[1]) / 1024
    return 0.0


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
    for i in range(0, DOCS, 10):
        col.upsert([{"id": f"d{j}", "filter": {"grp": "g"}, "fields": {"mv": docs[j].tolist()}}
                    for j in range(i, min(i + 10, DOCS))])
        if (i + 10) % 100 == 0:
            print(f"  inserted {i + 10}/{DOCS}")
    print("setup done. Now: docker restart endee, then --run")


def run():
    pid = find_pid()
    docs = dataset()
    qs = queries()
    truth = []
    for q in qs:  # exact MaxSim: sum_j max_i dot(q_j, v_i); here one query token
        scores = np.einsum("d,nmd->nm", q[0], docs).max(axis=1)
        truth.append({f"d{i}" for i in np.argsort(-scores)[:LIMIT]})

    if pid is None:
        print("server process not on this host: memory is measured by memwatch.py on the server host\n")
    baseline = rss_anon_mb(pid) if pid else 0.0
    samples, stop = [baseline], threading.Event()

    def sampler():
        while pid and not stop.is_set():
            samples.append(rss_anon_mb(pid))
            time.sleep(0.02)

    lat, recalls, errors, lock = [], [], [], threading.Lock()

    def worker():
        s = requests.Session()
        s.headers["Authorization"] = TOKEN
        for _ in range(ROUNDS):
            for qi, q in enumerate(qs):
                body = {"fields": [{"mv": {"query": q.tolist(), "limit": LIMIT}}],
                        "filter": [{"grp": {"$eq": "g"}}]}
                t = time.perf_counter()
                r = s.post(f"{BASE}/collection/{COLL}/search", json=body, timeout=300)
                ms = (time.perf_counter() - t) * 1000
                if r.status_code != 200:
                    with lock:
                        errors.append(f"{r.status_code} {r.text[:200]}")
                    continue
                raw = msgpack.unpackb(r.content, raw=False, strict_map_key=False)
                got = {raw[0].get(h[0], [str(h[0])])[0] for h in raw[1].get("mv", [])}
                with lock:
                    lat.append(ms)
                    recalls.append(len(got & truth[qi]) / LIMIT)

    st = threading.Thread(target=sampler)
    st.start()
    workers = [threading.Thread(target=worker) for _ in range(THREADS)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    time.sleep(0.2)
    stop.set()
    st.join()

    peak = max(samples)
    print(f"server pid {pid}; {THREADS} concurrent threads x {ROUNDS * QUERIES} single-token queries; "
          f"{DOCS} docs x {MEMBERS} members x dim {DIM} float32 "
          f"(~{DOCS * MEMBERS * DIM * 4 / 2**20:.0f} MB of members per query)\n")
    if pid:
        print(f"RssAnon baseline     {baseline:8.1f} MB")
        print(f"RssAnon peak         {peak:8.1f} MB")
        print(f"peak growth          {peak - baseline:8.1f} MB")
        print(f"after load           {rss_anon_mb(pid):8.1f} MB")
    print(f"median latency       {statistics.median(lat):8.1f} ms   (p90 "
          f"{sorted(lat)[int(len(lat) * 0.9)]:.1f} ms, {len(lat)} queries)")
    print(f"recall@10            {statistics.mean(recalls):8.2f}")
    if errors:
        print(f"errors: {len(errors)}; first: {errors[0]}")


if "--setup" in sys.argv:
    setup()
elif "--run" in sys.argv:
    run()
else:
    print(__doc__)
