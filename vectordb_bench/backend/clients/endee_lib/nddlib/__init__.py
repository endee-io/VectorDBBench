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
wheel); set ``NDD_CAPI_PATH`` to override.
"""

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
    # errors
    "NddError",
    "NotFoundError",
    "ValidationError",
    "TierError",
    "InternalError",
    "UnknownOpError",
]
