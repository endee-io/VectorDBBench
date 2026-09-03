"""In-process Endee (ndd) — a drop-in for the networked `endee` pip client.

`navigation` normally reaches the Endee vector DB over HTTP (the pip `endee`
client → a standalone `ndd` server on :8080). This package embeds the same
engine **in-process** via the vendored C ABI shared library `libndd_capi.so`
(copied here from `/home/debian/ndd/build`), driven through `ctypes`. No server,
no socket.

It exposes the small slice of the pip client's Collections API that
`nav/endee_db.py` and `tools/build_endee.py` use — `Endee(token)`,
`get_collection`, `create_collection`, and on the collection object
`search` / `upsert` / `describe` — reproducing the client's observable
behaviour (cosine L2-normalization, zlib+JSON meta, msgpack wire shapes) so it
is a byte-for-byte drop-in. Select it with `NDD_LOCAL=1`; everything else is
configured by the env file (see `ndd.env.example` / `load_env_file`).

Engine C ABI reference: `ndd_capi.h` (vendored here) and
`/home/debian/ndd/docs/capi.md` §Python.
"""
from __future__ import annotations

import atexit
import ctypes
import json
import os
import threading
import zlib

import msgpack
import numpy as np

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))

# Reserved meta key: the pip client stashes each cosine vector's L2 norm here at
# upsert time (the engine stores unit vectors only). Hidden from returned meta.
_NORMS_KEY = "internal_"


# ─────────────────────────────────────────────────────────────────────────────
# Env file — the single editable place for every engine knob (loaded BEFORE the
# .so is mapped, because most knobs are read once at static-init via getenv and
# then cached). The file is AUTHORITATIVE: it overrides any already-exported
# NDD_* so a stale environment can't silently win. NDD_LOCAL / NDD_ENV_FILE are
# the only bootstrap vars and must live in the real environment.
# ─────────────────────────────────────────────────────────────────────────────

def load_env_file(path: str | None = None) -> None:
    """Apply a dotenv-style KEY=VALUE file into os.environ (file wins)."""
    if path is None:
        path = os.environ.get("NDD_ENV_FILE") or os.path.join(_PKG_DIR, "ndd.env")
    if not os.path.exists(path):
        return
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]                       # quoted: keep literal
            else:
                for sep in (" #", "\t#"):             # strip an inline comment
                    i = val.find(sep)
                    if i != -1:
                        val = val[:i]
                val = val.rstrip()
            if key:
                os.environ[key] = val   # file authoritative


# Apply the env file at import time — this runs before any ctypes.CDLL below,
# so the engine's load-time getenv sees every value.
load_env_file()


def _capi_so_path() -> str:
    return os.environ.get("NDD_CAPI_SO") or os.path.join(_PKG_DIR, "libndd_capi.so")


def _namespace() -> str:
    return os.environ.get("NDD_NAMESPACE", "autonav")


# ─────────────────────────────────────────────────────────────────────────────
# ctypes binding (mirrors /home/debian/ndd/docs/capi.md §Python)
# ─────────────────────────────────────────────────────────────────────────────

class _NddOpenOptions(ctypes.Structure):
    _fields_ = [
        ("data_dir", ctypes.c_char_p),
        ("skip_sanity", ctypes.c_int),
        ("save_on_shutdown", ctypes.c_int),
        ("vector_cache_max_bytes", ctypes.c_size_t),
        ("parallel_insert_threads", ctypes.c_int),
    ]


_LOCK = threading.Lock()
_LIB = None
_HANDLE = None
_DATA_DIR = None            # the data_dir this process committed to (one per process)
_DATA_DIR_KEEP = None       # keep the encoded bytes alive for the c_char_p


def _lib():
    global _LIB
    if _LIB is None:
        lib = ctypes.CDLL(_capi_so_path())
        lib.ndd_open.restype = ctypes.c_void_p
        lib.ndd_open.argtypes = [ctypes.POINTER(_NddOpenOptions), ctypes.POINTER(ctypes.c_char_p)]
        lib.ndd_close.restype = None
        lib.ndd_close.argtypes = [ctypes.c_void_p]
        lib.ndd_free.restype = None
        lib.ndd_free.argtypes = [ctypes.c_void_p]
        lib.ndd_version.restype = ctypes.c_char_p
        lib.ndd_version.argtypes = []
        lib.ndd_call.restype = ctypes.c_int
        lib.ndd_call.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.c_char_p, ctypes.c_size_t,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)), ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_size_t),
        ]
        _LIB = lib
    return _LIB


def version() -> str:
    return _lib().ndd_version().decode()


def _open_handle():
    """ndd_open once per process, from env-file config. Handle is thread-safe."""
    global _HANDLE, _DATA_DIR, _DATA_DIR_KEEP
    if _HANDLE is not None:
        return _HANDLE
    with _LOCK:
        if _HANDLE is not None:
            return _HANDLE
        data_dir = os.environ.get("NDD_DATA_DIR")
        if not data_dir:
            raise RuntimeError("nav.ndd_lib: NDD_DATA_DIR is required (set it in ndd.env)")
        _DATA_DIR_KEEP = data_dir.encode()
        opts = _NddOpenOptions(
            data_dir=_DATA_DIR_KEEP,
            skip_sanity=int(os.environ.get("NDD_SKIP_SANITY", "0") or 0),
            save_on_shutdown=int(os.environ.get("NDD_SAVE_ON_SHUTDOWN", "1") or 0),
            vector_cache_max_bytes=int(os.environ.get("NDD_VECTOR_CACHE_MAX_BYTES", "0") or 0),
            parallel_insert_threads=int(os.environ.get("NDD_NUM_PARALLEL_INSERTS", "0") or 0),
        )
        err = ctypes.c_char_p()
        lib = _lib()
        handle = lib.ndd_open(ctypes.byref(opts), ctypes.byref(err))
        if not handle:
            msg = err.value.decode() if err.value else "unknown error"
            if err.value is not None:
                lib.ndd_free(err)
            raise RuntimeError(f"nav.ndd_lib: ndd_open failed: {msg}")
        _HANDLE = handle
        _DATA_DIR = data_dir
        return _HANDLE


@atexit.register
def _shutdown():
    global _HANDLE
    if _HANDLE is not None and _LIB is not None:
        _LIB.ndd_close(_HANDLE)   # flushes (save_on_shutdown) + joins threads
        _HANDLE = None


def _call(op: str, cid: str | None, req: bytes = b""):
    """Invoke ndd_call; return (code, response_bytes, error_message)."""
    handle = _open_handle()
    lib = _lib()
    if isinstance(req, str):
        req = req.encode()
    out = ctypes.POINTER(ctypes.c_ubyte)()
    out_len = ctypes.c_size_t(0)
    msg = ctypes.c_char_p()
    msg_len = ctypes.c_size_t(0)
    code = lib.ndd_call(
        handle, op.encode(), (cid.encode() if cid else None), req, len(req),
        ctypes.byref(out), ctypes.byref(out_len),
        ctypes.byref(msg), ctypes.byref(msg_len),
    )
    body = b""
    if out and out_len.value:
        body = bytes(ctypes.cast(out, ctypes.POINTER(ctypes.c_ubyte * out_len.value)).contents)
    err = msg.value.decode() if msg.value else ""
    if out:
        lib.ndd_free(out)
    if msg.value is not None:
        lib.ndd_free(msg)
    return code, body, err


def _ok(op: str, cid: str | None, req: bytes = b"") -> bytes:
    """_call that raises on any non-zero (caller-fixable or engine) code."""
    code, body, err = _call(op, cid, req)
    if code != 0:
        raise RuntimeError(f"ndd {op} [{cid}] failed (code {code}): {err}")
    return body


# ─────────────────────────────────────────────────────────────────────────────
# helpers mirroring the pip client (endee/collection.py, endee/compression.py)
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_dense(vec, space_type: str):
    """L2-normalize a dense vector for cosine; return (list, norm). Non-cosine → unchanged."""
    v = np.asarray(vec, dtype=np.float32)
    if space_type == "cosine":
        n = float(np.sqrt(float(np.dot(v, v)))) or 1e-10
        n = max(n, 1e-10)
        return (v / n).tolist(), n
    return v.tolist(), 1.0


def _decode_meta(m):
    """Best-effort meta decode: zlib+JSON (the store's form), then plain JSON, then raw.

    Uses stdlib json+zlib only (orjson.dumps output is standard JSON) — no orjson dep.
    """
    if m is None or m == b"" or m == "":
        return {}
    if isinstance(m, (bytes, bytearray)):
        b = bytes(m)
        try:
            return json.loads(zlib.decompress(b))
        except Exception:
            try:
                return json.loads(b)
            except Exception:
                return b
    if isinstance(m, str):
        try:
            return json.loads(m)
        except Exception:
            return m
    return m


def _encode_meta(meta_dict: dict) -> bytes:
    """Mirror the pip client json_zip: zlib.compress of compact JSON bytes."""
    return zlib.compress(json.dumps(meta_dict, separators=(",", ":")).encode("utf-8"))


def _decode_filter(f):
    if not f:
        return {}
    if isinstance(f, (bytes, bytearray)):
        f = bytes(f).decode("utf-8", "replace")
    if isinstance(f, str):
        try:
            return json.loads(f)
        except Exception:
            return f
    return f


# ─────────────────────────────────────────────────────────────────────────────
# public API — Endee / Collection drop-ins
# ─────────────────────────────────────────────────────────────────────────────

class Endee:
    """Drop-in for `endee.Endee`, backed by the in-process C ABI."""

    def __init__(self, token=None, *args, **kwargs):
        self.token = token          # accepted & ignored (no auth in library mode)
        self.namespace = _namespace()
        _open_handle()              # open the process handle eagerly on first client

    def _cid(self, name: str) -> str:
        return f"{self.namespace}/{name}"

    def create_collection(self, name: str, fields: list, size_in_millions=1) -> dict:
        """`size_in_millions` sets the collection's element CAPACITY, which costs real disk.

        Measured on this engine: `vectors/vec_cls.idx` is pre-allocated at **140 bytes per
        element of capacity**, independent of how many elements are actually inserted — 140 MB
        at size_in_millions=1, 280 MB at 2, 14 MB at the engine's own 100k default. On a 29 GB
        Pi card with three z-level collections per AOI, the difference between 1M and 100k
        capacity is 378 MB per AOI, which is about a third of a 5x5 km AOI's total footprint.

        Pass **None** to omit the field entirely and inherit `NDD_MAX_ELEMENTS` from ndd.env
        (100k here); collections auto-grow past it via NDD_MAX_ELEMENTS_INCREMENT. The default
        stays 1 so `tools/build_endee.py` — which built the validated 139k-tile DB — is
        unchanged. Values below 1 are rejected by the engine, so sizing below a million is only
        reachable by omitting the field.
        """
        # name is carried by the collection_id, NOT the body.
        body_d: dict = {"fields": fields}
        if size_in_millions is not None:
            body_d["size_in_millions"] = size_in_millions
        body = json.dumps(body_d)
        # raises on non-zero (incl. already-exists conflict) so build_endee's
        # except→reuse branch works exactly as with the pip client.
        _ok("create_collection", self._cid(name), body)
        return {"name": name}

    def get_collection(self, name: str) -> "Collection":
        body = _ok("describe_collection", self._cid(name))
        meta = json.loads(body)
        return Collection(name, self._cid(name), meta)

    def list_collections(self) -> list:
        body = _ok("list_collections", self.namespace)
        return json.loads(body).get("collections", [])

    def delete_collection(self, name: str) -> dict:
        """Drop a collection. Used by the AOI loader to replace one area's data without
        touching the others sharing this data dir (they are separated by name prefix)."""
        _ok("delete_collection", self._cid(name))
        return {"name": name, "deleted": True}

    def __str__(self):
        return str(self.token)


class Collection:
    """Drop-in for `endee.collection.Collection` (the subset nav uses)."""

    def __init__(self, name: str, cid: str, metadata: dict):
        self.name = name
        self._cid = cid
        self.fields = metadata.get("fields", []) or []
        # name -> {type, space_type, dimension} (drives cosine normalization)
        self._field_idx = {}
        for f in self.fields:
            params = f.get("params", {}) or {}
            self._field_idx[f["name"]] = {
                "type": f.get("type", "vector"),
                "space_type": params.get("space_type", "cosine"),
                "dimension": params.get("dimension", 0),
            }

    def describe(self) -> dict:
        return json.loads(_ok("describe_collection", self._cid))

    # ── search ──────────────────────────────────────────────────────────────
    def search(self, fields, filter=None, ef_search=256,
               prefilter_cardinality_threshold=None, filter_boost_percentage=None):
        """One-request per-field search. Returns {"results": {field: [hit, ...]}}.

        Each hit is {"id", "similarity", "meta", "filter"} — same shape the pip
        client returns (so nav/endee_db.py needs no change).
        """
        fields_array = []
        field_limits = {}
        for fname, fdata in fields.items():
            if not (isinstance(fdata, dict) and "query" in fdata):
                raise ValueError(f"search field '{fname}' must be {{'query': ...}}")
            cfg = dict(fdata)
            query = cfg["query"]
            fld = self._field_idx.get(fname, {})
            # L2-normalize a dense cosine query (mirrors the pip client; no-op if unit)
            if (fld.get("type") == "vector" and fld.get("space_type", "cosine") == "cosine"
                    and isinstance(query, (list, tuple)) and query
                    and not isinstance(query[0], (list, tuple))):
                query, _ = _normalize_dense(query, "cosine")
            limit = int(cfg.get("limit", 10))
            ef = int(cfg.get("ef_search", ef_search))
            field_limits[fname] = limit
            fields_array.append({fname: {"query": query, "limit": limit, "ef_search": ef}})

        payload = {"fields": fields_array}
        if filter is not None:
            payload["filter"] = filter
        if prefilter_cardinality_threshold is not None or filter_boost_percentage is not None:
            payload["filter_params"] = {
                "prefilter_threshold": prefilter_cardinality_threshold
                if prefilter_cardinality_threshold is not None else 10000,
                "boost_percentage": filter_boost_percentage
                if filter_boost_percentage is not None else 0,
            }

        body = _ok("search", self._cid, json.dumps(payload))
        # SearchResult = [objects_map, results_map]; objects keyed by INTEGER internal id.
        objects_map, results_map = msgpack.unpackb(body, raw=False, strict_map_key=False)

        def ext(iid):
            o = objects_map.get(iid)
            if o is None:
                return {"id": str(iid), "meta": {}, "filter": {}}
            meta = _decode_meta(o[1]) if len(o) > 1 else {}
            if isinstance(meta, dict):
                meta.pop(_NORMS_KEY, None)
            return {"id": o[0], "meta": meta,
                    "filter": _decode_filter(o[2]) if len(o) > 2 else {}}

        per_field = {}
        for fname in fields:
            hits = []
            for hit in (results_map.get(fname, []) or [])[:field_limits[fname]]:
                d = ext(hit[0])
                d["similarity"] = float(hit[1])
                hits.append(d)
            per_field[fname] = hits
        return {"results": per_field}

    # ── upsert (rebuild path; msgpack because meta is binary) ─────────────────
    def upsert(self, objects: list) -> dict:
        wire = []
        for obj in objects:
            oid = str(obj["id"])
            flt = obj.get("filter")
            if flt is None:
                filter_str = ""
            elif isinstance(flt, str):
                filter_str = flt
            else:
                filter_str = json.dumps(flt, separators=(",", ":"))

            vectors, sparses, multi_vectors, norms = {}, {}, {}, {}
            for fname, fdata in (obj.get("fields") or {}).items():
                cfg = self._field_idx.get(fname, {"type": "vector", "space_type": "cosine"})
                if cfg["type"] == "vector":
                    vec, n = _normalize_dense(fdata, cfg.get("space_type", "cosine"))
                    vectors[fname] = vec
                    if cfg.get("space_type", "cosine") == "cosine":
                        norms[fname] = n
                elif cfg["type"] == "sparse":
                    idx = [int(i) for i in fdata.get("indices", [])]
                    val = [float(v) for v in fdata.get("values", [])]
                    sparses[fname] = [idx, val]
                else:  # multi_vector
                    arr = [list(map(float, v)) for v in fdata]
                    multi_vectors[fname] = arr

            raw_meta = obj.get("meta")
            if isinstance(raw_meta, (bytes, bytearray)):
                meta_bytes = bytes(raw_meta)
            else:
                md = dict(raw_meta) if isinstance(raw_meta, dict) else {}
                if norms:
                    md[_NORMS_KEY] = norms
                meta_bytes = _encode_meta(md)

            wire.append([oid, meta_bytes, filter_str, vectors, sparses, multi_vectors])

        payload = msgpack.packb([wire], use_bin_type=True, use_single_float=True)
        body = _ok("add_objects_msgpack", self._cid, payload)
        try:
            return json.loads(body)
        except Exception:
            return {"status": body.decode("utf-8", "replace")}

    # ── fetch full objects by id (msgpack ObjectBatch) ────────────────────────
    def get_objects(self, ids: list) -> list:
        body = _ok("get_objects", self._cid,
                   json.dumps({"ids": [str(i) for i in ids]}))
        batch = msgpack.unpackb(body, raw=False, strict_map_key=False)
        objects = (batch[0] if isinstance(batch, list) and batch else []) or []
        out = []
        for o in objects:
            meta = _decode_meta(o[1]) if len(o) > 1 else {}
            if isinstance(meta, dict):
                meta.pop(_NORMS_KEY, None)
            out.append({
                "id": o[0],
                "meta": meta,
                "filter": _decode_filter(o[2]) if len(o) > 2 else {},
                "vectors": {k: list(v) for k, v in (o[3] or {}).items()} if len(o) > 3 else {},
            })
        return out

    def __str__(self):
        return self.name
