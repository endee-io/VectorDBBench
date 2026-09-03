"""Typed value objects for the ndd data model, plus the msgpack codecs that
move them across the C ABI.

The wire layouts these encode/decode are specified in docs/capi.md sections 8
and 9, and declared in src/utils/msgpack_ndd.hpp. Every msgpack struct is a
*positional* array, so element order here is load-bearing.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field as _dc_field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from . import _msgpack as mp
from .errors import ValidationError

Scalar = Union[str, int, float, bool]
Vector = Sequence[float]


# ---------------------------------------------------------------------------
# field specifications (create_collection)
# ---------------------------------------------------------------------------

SPACE_TYPES = ("cosine", "l2", "ip")
PRECISIONS = ("float32", "float16", "int16", "int8", "int8e", "binary")
POOLINGS = ("mean", "max")
SPARSE_MODELS = ("default", "endee_bm25")


@dataclass
class VectorField:
    """A dense HNSW vector field.

    Ranges mirror the engine's validation (docs/capi.md section 7) so a bad
    value is reported here, with the offending field named, instead of coming
    back as a generic code-2 message.
    """

    name: str
    dimension: int
    space_type: str = "cosine"
    precision: str = "int16"
    M: int = 16
    ef_con: int = 128

    type: str = _dc_field(default="vector", init=False, repr=False)

    def to_json(self) -> dict:
        _check_field_name(self.name)
        _check_vector_params(self)
        return {
            "name": self.name,
            "type": "vector",
            "params": {
                "dimension": self.dimension,
                "space_type": self.space_type,
                "precision": self.precision,
                "M": self.M,
                "ef_con": self.ef_con,
            },
        }


@dataclass
class MultiVectorField:
    """A multi-vector (ColBERT-style late interaction) field. Members are pooled
    into one HNSW vector; the raw members drive stage-2 MaxSim reranking."""

    name: str
    dimension: int
    space_type: str = "cosine"
    precision: str = "int16"
    M: int = 16
    ef_con: int = 128
    pooling: str = "mean"

    type: str = _dc_field(default="multi_vector", init=False, repr=False)

    def to_json(self) -> dict:
        _check_field_name(self.name)
        _check_vector_params(self)
        if self.pooling not in POOLINGS:
            raise ValidationError(
                f"field {self.name!r}: pooling must be one of {POOLINGS}, got {self.pooling!r}"
            )
        return {
            "name": self.name,
            "type": "multi_vector",
            "params": {
                "dimension": self.dimension,
                "space_type": self.space_type,
                "precision": self.precision,
                "M": self.M,
                "ef_con": self.ef_con,
                "pooling": self.pooling,
            },
        }


@dataclass
class SparseField:
    """A sparse (inverted-index) field."""

    name: str
    sparse_model: str = "default"

    type: str = _dc_field(default="sparse", init=False, repr=False)

    def to_json(self) -> dict:
        _check_field_name(self.name)
        if self.sparse_model not in SPARSE_MODELS:
            raise ValidationError(
                f"field {self.name!r}: sparse_model must be one of {SPARSE_MODELS}, "
                f"got {self.sparse_model!r}"
            )
        return {
            "name": self.name,
            "type": "sparse",
            "sparse_model": self.sparse_model,
        }


FieldSpec = Union[VectorField, MultiVectorField, SparseField]


def _check_field_name(name: str) -> None:
    # Stricter than db/collection names: no hyphen (docs/capi.md section 6a).
    if not name:
        raise ValidationError("field name cannot be empty")
    if not all(c.isalnum() and c.isascii() or c == "_" for c in name):
        raise ValidationError(
            f"field name {name!r} must contain only ASCII letters, digits, or underscore"
        )


def _check_vector_params(spec) -> None:
    if not 2 <= spec.dimension <= 16384:
        raise ValidationError(
            f"field {spec.name!r}: dimension must be 2..16384, got {spec.dimension}"
        )
    if spec.space_type not in SPACE_TYPES:
        raise ValidationError(
            f"field {spec.name!r}: space_type must be one of {SPACE_TYPES}, got {spec.space_type!r}"
        )
    if spec.precision not in PRECISIONS:
        raise ValidationError(
            f"field {spec.name!r}: precision must be one of {PRECISIONS}, got {spec.precision!r}"
        )
    if not 4 <= spec.M <= 512:
        raise ValidationError(f"field {spec.name!r}: M must be 4..512, got {spec.M}")
    if not 8 <= spec.ef_con <= 4096:
        raise ValidationError(
            f"field {spec.name!r}: ef_con must be 8..4096, got {spec.ef_con}"
        )


# ---------------------------------------------------------------------------
# object model
# ---------------------------------------------------------------------------


@dataclass
class SparseVector:
    """A sparse value: parallel index/value arrays."""

    indices: Sequence[int]
    values: Sequence[float]

    def __post_init__(self) -> None:
        if len(self.indices) != len(self.values):
            raise ValidationError(
                f"sparse vector has {len(self.indices)} indices but "
                f"{len(self.values)} values; they must be parallel"
            )


@dataclass
class Object:
    """One stored record.

    An object may carry values for **any subset** of the collection's fields.
    ``id`` is the upsert key: re-adding the same id overwrites.

    ``meta`` is opaque bytes stored verbatim (the engine never compresses it).
    For convenience, passing a ``str`` encodes it as UTF-8, and ``metadata``
    (any JSON-serializable value) is stored as UTF-8 JSON. Use
    :meth:`json_metadata` to read it back.

    ``filter`` is the per-object payload filter: a flat mapping of field to
    scalar, e.g. ``{"category": "docs", "year": 2020}``.
    """

    id: str
    vectors: Mapping[str, Vector] = _dc_field(default_factory=dict)
    sparses: Mapping[str, SparseVector] = _dc_field(default_factory=dict)
    multi_vectors: Mapping[str, Sequence[Vector]] = _dc_field(default_factory=dict)
    filter: Optional[Mapping[str, Scalar]] = None
    meta: bytes = b""
    metadata: Any = None

    def __post_init__(self) -> None:
        if self.metadata is not None:
            if self.meta:
                raise ValidationError(
                    f"object {self.id!r}: pass either meta (raw bytes) or metadata "
                    "(JSON-serializable), not both"
                )
            self.meta = json.dumps(self.metadata).encode("utf-8")
        elif isinstance(self.meta, str):
            self.meta = self.meta.encode("utf-8")

    def json_metadata(self) -> Any:
        """Decode ``meta`` as UTF-8 JSON. Returns ``None`` when meta is empty,
        and raises ``ValueError`` if it is not JSON (it is opaque bytes, so it
        may well be something else)."""
        if not self.meta:
            return None
        return json.loads(self.meta.decode("utf-8"))

    def text_metadata(self) -> str:
        """Decode ``meta`` as UTF-8 text."""
        return self.meta.decode("utf-8")

    def filter_json(self) -> str:
        """The per-object filter as the JSON-object string the wire expects."""
        if not self.filter:
            return ""
        if not isinstance(self.filter, Mapping):
            raise ValidationError(
                f"object {self.id!r}: filter must be a mapping of field to scalar, "
                f"got {type(self.filter).__name__}"
            )
        return json.dumps(dict(self.filter))


@dataclass
class Hit:
    """One ranked search hit, already resolved to its external id.

    ``score`` is a **similarity: higher is better**, and hits arrive ranked
    best-first. That holds for every space type and field type - a cosine exact
    match scores ~1.0, and l2 fields are ranked the same direction.

    Scores are **not comparable across fields**: a BM25 score and a cosine
    similarity are different scales. Fuse by rank instead - see
    :meth:`SearchResults.rrf`.

    ``internal_id`` is the engine's numeric id, the join key inside one search
    response. It is not stable across rebuilds; use ``id`` to refer to an object.
    """

    id: str
    score: float
    internal_id: int
    meta: bytes = b""
    filter: Optional[dict] = None

    def json_metadata(self) -> Any:
        """Decode ``meta`` as UTF-8 JSON (``None`` when empty)."""
        if not self.meta:
            return None
        return json.loads(self.meta.decode("utf-8"))

    def text_metadata(self) -> str:
        return self.meta.decode("utf-8")


class SearchResults(Dict[str, List[Hit]]):
    """Per-field ranked hit lists: ``results["text_emb"][0]`` is that field's
    best hit.

    The engine performs **no cross-field fusion** - each field is queried
    independently and returns its own ranked list. Merge them client-side; see
    :meth:`rrf` for a ready-made Reciprocal Rank Fusion.
    """

    def rrf(self, k: int = 60, limit: Optional[int] = None) -> List[Hit]:
        """Reciprocal Rank Fusion across every field in this result.

        Each hit scores ``sum(1 / (k + rank))`` over the fields it appears in
        (rank is 1-based). The returned hits carry that fused score, sorted best
        first. ``k`` damps the influence of top ranks; 60 is the common default.
        """
        fused: Dict[str, float] = {}
        best: Dict[str, Hit] = {}
        for hits in self.values():
            for rank, hit in enumerate(hits, start=1):
                fused[hit.id] = fused.get(hit.id, 0.0) + 1.0 / (k + rank)
                best.setdefault(hit.id, hit)
        order = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)
        if limit is not None:
            order = order[:limit]
        out = []
        for external_id, score in order:
            template = best[external_id]
            out.append(
                Hit(
                    id=external_id,
                    score=score,
                    internal_id=template.internal_id,
                    meta=template.meta,
                    filter=template.filter,
                )
            )
        return out


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def normalize(vector: Vector) -> List[float]:
    """L2-normalize a vector.

    Cosine fields require this: the engine stores **no norms**, so the client
    must normalize both inserts and queries. A zero vector is returned as-is
    (there is no meaningful direction to scale it to).
    """
    total = 0.0
    for v in vector:
        total += v * v
    norm = math.sqrt(total)
    if norm == 0.0:
        return [float(v) for v in vector]
    return [float(v) / norm for v in vector]


# ---------------------------------------------------------------------------
# msgpack encoding / decoding
# ---------------------------------------------------------------------------


def encode_object_batch(objects: Iterable[Object]) -> bytes:
    """Encode objects as a msgpack ``ObjectBatch``.

    Layout (docs/capi.md section 8) - note ``multi_vectors`` is deliberately the
    trailing element so older readers still decode the first five::

        Sparse      = [indices, values]
        Object      = [id, meta, filter, vectors, sparses, multi_vectors]
        ObjectBatch = [[Object, ...]]
    """
    objects = list(objects)
    out = bytearray()
    mp.pack_array_header(out, 1)              # ObjectBatch: 1-element struct
    mp.pack_array_header(out, len(objects))   # the objects vector

    for obj in objects:
        if not isinstance(obj, Object):
            raise ValidationError(
                f"expected an nddlib.Object, got {type(obj).__name__}"
            )
        if not obj.id:
            raise ValidationError("every object requires a non-empty id")

        mp.pack_array_header(out, 6)
        mp.pack_str(out, obj.id)
        mp.pack_bin(out, bytes(obj.meta))
        mp.pack_str(out, obj.filter_json())

        mp.pack_map_header(out, len(obj.vectors))
        for name, values in obj.vectors.items():
            mp.pack_str(out, name)
            mp.pack_float_array(out, values)

        mp.pack_map_header(out, len(obj.sparses))
        for name, sparse in obj.sparses.items():
            if not isinstance(sparse, SparseVector):
                sparse = SparseVector(*sparse)
            mp.pack_str(out, name)
            mp.pack_array_header(out, 2)
            mp.pack_uint_array(out, sparse.indices)
            mp.pack_float_array(out, sparse.values)

        mp.pack_map_header(out, len(obj.multi_vectors))
        for name, members in obj.multi_vectors.items():
            mp.pack_str(out, name)
            mp.pack_array_header(out, len(members))
            for member in members:
                mp.pack_float_array(out, member)

    return bytes(out)


def _decode_filter(raw: str) -> Optional[dict]:
    """A filter comes back as a JSON-object string; empty means unset. It is
    returned raw-on-failure rather than raising, since it is user-supplied."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return {"_raw": raw}


def decode_object_batch(payload: bytes) -> List[Object]:
    """Decode a msgpack ``ObjectBatch`` (the ``get_objects`` response)."""
    decoded = mp.unpackb(payload)
    if not isinstance(decoded, list) or not decoded:
        return []
    objects = []
    for row in decoded[0]:
        # Tolerate a short row: multi_vectors is a trailing, optional element.
        oid = row[0]
        meta = row[1] if len(row) > 1 else b""
        filt = row[2] if len(row) > 2 else ""
        vectors = row[3] if len(row) > 3 else {}
        sparses = row[4] if len(row) > 4 else {}
        multi = row[5] if len(row) > 5 else {}
        objects.append(
            Object(
                id=oid,
                vectors={k: list(v) for k, v in (vectors or {}).items()},
                sparses={
                    k: SparseVector(list(v[0]), list(v[1]))
                    for k, v in (sparses or {}).items()
                },
                multi_vectors={
                    k: [list(m) for m in v] for k, v in (multi or {}).items()
                },
                filter=_decode_filter(filt),
                meta=bytes(meta or b""),
            )
        )
    return objects


def decode_search_result(payload: bytes) -> SearchResults:
    """Decode a msgpack ``SearchResult`` into per-field :class:`Hit` lists.

    Wire layout (docs/capi.md section 9)::

        ObjectMeta   = [id, meta, filter]
        SearchHit    = [internal_id, score]
        SearchResult = [objects: map<u32, ObjectMeta>,
                        results: map<str, [SearchHit, ...]>]

    Hits carry only the *internal* numeric id, so each is joined against the
    ``objects`` map to recover the external id and metadata.
    """
    decoded = mp.unpackb(payload)
    results = SearchResults()
    if not isinstance(decoded, list) or len(decoded) < 2:
        return results

    objects = decoded[0] or {}
    per_field = decoded[1] or {}

    for field_name, hits in per_field.items():
        resolved: List[Hit] = []
        for hit in hits:
            internal_id, score = hit[0], hit[1]
            meta_row = objects.get(internal_id)
            if meta_row is None:
                # Metadata hydration is best-effort on the engine side; keep the
                # hit rather than dropping a real result.
                resolved.append(Hit(id="", score=score, internal_id=internal_id))
                continue
            resolved.append(
                Hit(
                    id=meta_row[0],
                    score=score,
                    internal_id=internal_id,
                    meta=bytes(meta_row[1] or b""),
                    filter=_decode_filter(meta_row[2] if len(meta_row) > 2 else ""),
                )
            )
        results[field_name] = resolved
    return results
