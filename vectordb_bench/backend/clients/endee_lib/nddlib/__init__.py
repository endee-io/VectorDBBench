"""nddlib - Python client for the embedded Endee (ndd) vector database.

This drives the engine **in-process** through libndd_capi: dense HNSW search,
multi-vector (ColBERT-style) late interaction, sparse BM25 retrieval, and
payload filtering, with no server and no auth.

    import nddlib

    with nddlib.Database("./data") as db:
        docs = db.create_collection("docs", fields=[
            nddlib.VectorField("text_emb", dimension=384, space_type="cosine"),
            nddlib.SparseField("keywords", sparse_model="endee_bm25"),
        ])

        docs.add(nddlib.Object(
            id="doc1",
            vectors={"text_emb": nddlib.normalize(embedding)},
            sparses={"keywords": nddlib.SparseVector([5, 42], [0.9, 0.5])},
            filter={"category": "docs", "year": 2020},
            metadata={"title": "Intro to vector search"},
        ))

        results = docs.search(
            {"text_emb": nddlib.normalize(query_embedding),
             "keywords": nddlib.SparseVector([5], [1.0])},
            filter=[{"year": {"$gte": 2020}}],
            limit=10,
        )
        for hit in results.rrf():          # no server-side fusion; merge here
            print(hit.id, hit.score)

The library is located automatically (a sibling ``build/`` directory, or the
wheel); set ``NDD_CAPI_PATH`` (or ``NDD_CAPI_SO``) to override.

Importing this package applies an ``ndd.env`` config file if one is found, so
every engine knob in it is in the environment before libndd_capi is mapped -
see :mod:`nddlib._env`. ``Database()`` then defaults its data dir, namespace and
open options from that environment, so the file alone is enough to configure a
deployment::

    import nddlib
    db = nddlib.Database()          # data_dir / db / open options from ndd.env
    print(nddlib.env_file)          # which file was applied, or None
"""

# Must run before anything can dlopen the shared library: the engine caches most
# NDD_* knobs at static-init time. Importing ._ffi does not map the library (it
# is loaded lazily on first use), but keep this first regardless.
from ._env import env_bool, env_int, load_env_file

#: Path of the ``ndd.env`` applied at import, or ``None`` if none was found.
env_file = load_env_file()

from ._ffi import find_library, version
from .client import Collection, Database, Query
from .errors import (
    InternalError,
    NddError,
    NotFoundError,
    TierError,
    UnknownOpError,
    ValidationError,
)
from .models import (
    Hit,
    MultiVectorField,
    Object,
    SearchResults,
    SparseField,
    SparseVector,
    VectorField,
    normalize,
)

__version__ = "0.1.0"

__all__ = [
    # client
    "Database",
    "Collection",
    "Query",
    # field specs
    "VectorField",
    "MultiVectorField",
    "SparseField",
    # data
    "Object",
    "SparseVector",
    "Hit",
    "SearchResults",
    # helpers
    "normalize",
    "version",
    "find_library",
    # configuration
    "load_env_file",
    "env_file",
    "env_bool",
    "env_int",
    # errors
    "NddError",
    "NotFoundError",
    "ValidationError",
    "TierError",
    "InternalError",
    "UnknownOpError",
]
