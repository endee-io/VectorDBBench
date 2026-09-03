"""The user-facing client: :class:`Database` and :class:`Collection`.

This binds the embedded engine (libndd_capi) - there is no server, no HTTP, and
no auth. A collection is addressed by a ``<db>/<collection>`` id, where ``<db>``
is purely a namespace string within the data directory.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from . import _ffi
from .errors import NddError, ValidationError
from .models import (
    FieldSpec,
    Hit,
    Object,
    SearchResults,
    SparseVector,
    Vector,
    decode_object_batch,
    decode_search_result,
    encode_object_batch,
    normalize,
)

__all__ = ["Database", "Collection", "Query", "normalize"]

_MAX_NAME_LENGTH = 200


def _check_name(kind: str, name: str) -> str:
    """Validate a db or collection name against the engine's rule (docs/capi.md
    section 6a): non-empty, <= 200 chars, ``[A-Za-z0-9_-]`` only.

    Checked client-side so a bad name is a clear Python error naming the part at
    fault, rather than a generic code-2 message from the engine.
    """
    if not name:
        raise ValidationError(f"{kind} name cannot be empty")
    if len(name) > _MAX_NAME_LENGTH:
        raise ValidationError(
            f"{kind} name {name!r} exceeds {_MAX_NAME_LENGTH} characters"
        )
    for c in name:
        if not (c.isascii() and (c.isalnum() or c in "_-")):
            raise ValidationError(
                f"{kind} name {name!r} may contain only ASCII letters, digits, "
                f"underscore, and hyphen (rejected {c!r})"
            )
    return name


# ---------------------------------------------------------------------------
# process-wide handle registry
# ---------------------------------------------------------------------------
#
# The C ABI allows only one data_dir per process, and re-opening the *same*
# data_dir hands back a second, independent CollectionManager over the same MDBX
# environment. Neither is something a caller should have to think about, so the
# handle is shared and reference-counted here: many Database objects over one
# data_dir are safe, and the underlying handle closes when the last one does.

_registry_lock = threading.Lock()
_open_handles: Dict[str, "_SharedHandle"] = {}


class _SharedHandle:
    __slots__ = ("handle", "data_dir", "refcount")

    def __init__(self, handle: int, data_dir: str) -> None:
        self.handle = handle
        self.data_dir = data_dir
        self.refcount = 0


def _acquire_handle(data_dir: str, **open_kwargs) -> _SharedHandle:
    resolved = os.path.abspath(os.path.expanduser(data_dir))
    with _registry_lock:
        shared = _open_handles.get(resolved)
        if shared is None:
            if _open_handles:
                already = next(iter(_open_handles))
                raise NddError(
                    f"cannot open {resolved!r}: {already!r} is already open in this "
                    "process. libndd_capi supports one data_dir per process - close "
                    "the existing Database first, or run the second one in another "
                    "process."
                )
            os.makedirs(resolved, exist_ok=True)
            shared = _SharedHandle(_ffi.open_handle(resolved, **open_kwargs), resolved)
            _open_handles[resolved] = shared
        shared.refcount += 1
        return shared


def _release_handle(shared: _SharedHandle) -> None:
    with _registry_lock:
        shared.refcount -= 1
        if shared.refcount > 0:
            return
        _open_handles.pop(shared.data_dir, None)
        handle, shared.handle = shared.handle, 0
    _ffi.close_handle(handle)


# ---------------------------------------------------------------------------
# search query
# ---------------------------------------------------------------------------


@dataclass
class Query:
    """A per-field search query with its own limit and recall setting.

    ``query`` is polymorphic and interpreted by shape, matching the engine:
    a flat sequence of numbers is a dense query, a sequence of sequences is a
    multi-vector query, and a :class:`SparseVector` (or ``{"indices","values"}``
    mapping) is a sparse query.

    ``ef_search`` is the recall lever - raise it to trade latency for recall. For
    a multi_vector field it also sets the candidate-set size that stage-2 MaxSim
    reranks, so it is the *only* recall lever there.
    """

    query: Any
    limit: int = 10
    ef_search: int = 128

    def to_json(self, field_name: str) -> dict:
        if not 1 <= self.limit <= 4096:
            raise ValidationError(
                f"field {field_name!r}: limit must be 1..4096, got {self.limit}"
            )
        return {
            field_name: {
                "query": _query_payload(field_name, self.query),
                "limit": self.limit,
                "ef_search": self.ef_search,
            }
        }


def _query_payload(field_name: str, query: Any) -> Any:
    """Convert a query value into the JSON shape the engine dispatches on."""
    if isinstance(query, SparseVector):
        return {"indices": list(query.indices), "values": [float(v) for v in query.values]}
    if isinstance(query, Mapping):
        if "indices" not in query or "values" not in query:
            raise ValidationError(
                f"field {field_name!r}: a mapping query must have 'indices' and 'values'"
            )
        return {
            "indices": list(query["indices"]),
            "values": [float(v) for v in query["values"]],
        }
    if isinstance(query, (str, bytes)):
        raise ValidationError(
            f"field {field_name!r}: query must be a vector, list of vectors, or "
            f"SparseVector, got {type(query).__name__}"
        )
    if isinstance(query, Sequence):
        if not query:
            raise ValidationError(f"field {field_name!r}: query cannot be empty")
        first = query[0]
        if isinstance(first, Sequence) and not isinstance(first, (str, bytes)):
            return [[float(v) for v in member] for member in query]  # multi-vector
        return [float(v) for v in query]                             # dense
    raise ValidationError(
        f"field {field_name!r}: unsupported query type {type(query).__name__}"
    )


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


class Collection:
    """Handle to one collection. Obtained from :class:`Database`; not
    constructed directly."""

    def __init__(self, database: "Database", name: str) -> None:
        self._db = database
        self._name = _check_name("collection", name)
        self._cid = f"{database.db}/{self._name}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def collection_id(self) -> str:
        """The full ``<db>/<collection>`` id passed across the ABI."""
        return self._cid

    def __repr__(self) -> str:
        return f"<Collection {self._cid!r}>"

    def _call(self, op: str, request: bytes = b"") -> bytes:
        return self._db._call(op, self._cid, request)

    def _call_json(self, op: str, body: Any = None) -> Any:
        payload = json.dumps(body).encode() if body is not None else b""
        raw = self._call(op, payload)
        return json.loads(raw) if raw else {}

    # -- schema -------------------------------------------------------------

    def describe(self) -> dict:
        """Live collection info: ``total_elements``, ``max_elements``, and a
        per-field descriptor including each field's ``element_count``."""
        return self._call_json("describe_collection")

    # -- writes -------------------------------------------------------------

    def add(self, objects: Union[Object, Iterable[Object]]) -> int:
        """Upsert objects; returns the number submitted.

        Each object is an atomic per-object upsert keyed on ``id`` - if any of
        its field values is malformed the whole object is rejected and nothing
        is written for it. An object may carry any subset of the collection's
        fields.

        For **cosine** fields you must L2-normalize dense and multi-vector
        values yourself (:func:`nddlib.normalize`); the engine stores no norms.
        """
        if isinstance(objects, Object):
            objects = [objects]
        batch = list(objects)
        if not batch:
            return 0
        raw = self._call("add_objects_msgpack", encode_object_batch(batch))
        return json.loads(raw).get("upserted", len(batch)) if raw else len(batch)

    def delete(self, object_id: str) -> None:
        """Delete one object by external id. Raises
        :class:`~nddlib.errors.NotFoundError` if it does not exist."""
        self._call_json("delete_object", {"id": str(object_id)})

    def delete_by_filter(self, filter: Sequence[Mapping[str, Any]]) -> int:
        """Delete every object matching a query filter; returns the count.

        ``filter`` is the query form: a list of single-field conditions, AND-ed,
        e.g. ``[{"year": {"$gte": 2020}}]``.
        """
        if not isinstance(filter, Sequence) or isinstance(filter, (str, bytes)):
            raise ValidationError("filter must be a list of conditions")
        result = self._call_json("delete_by_filter", {"filter": list(filter)})
        return result.get("deleted", 0)

    def update_filters(self, updates: Mapping[str, Mapping[str, Any]]) -> int:
        """Replace the payload-filter values of existing objects.

        ``updates`` maps object id to its new filter object, e.g.
        ``{"doc1": {"category": "archive"}}``. Returns the number updated;
        unknown ids are silently skipped by the engine.
        """
        body = {
            "updates": [
                {"id": str(oid), "filter": dict(values)}
                for oid, values in updates.items()
            ]
        }
        result = self._call_json("update_filters", body)
        return result.get("updated", 0)

    # -- reads --------------------------------------------------------------

    def get(self, ids: Union[str, Iterable[str]]) -> List[Object]:
        """Fetch objects by external id, with their vectors.

        Vectors of cosine fields come back **normalized** - the original
        magnitude is not retained. Missing ids are simply absent from the result.
        """
        if isinstance(ids, (str, bytes)):
            ids = [ids]
        id_list = [str(i) for i in ids]
        if not id_list:
            return []
        raw = self._call("get_objects", json.dumps({"ids": id_list}).encode())
        return decode_object_batch(raw)

    def search(
        self,
        fields: Mapping[str, Any],
        *,
        filter: Optional[Sequence[Mapping[str, Any]]] = None,
        limit: int = 10,
        ef_search: int = 128,
        prefilter_threshold: Optional[float] = None,
        boost_percentage: Optional[float] = None,
    ) -> SearchResults:
        """Search one or more fields and return per-field ranked hit lists.

        ``fields`` maps field name to a query. A query may be a bare vector, a
        list of vectors (multi-vector), a :class:`SparseVector`, or a
        :class:`Query` when that field needs its own ``limit``/``ef_search``.
        ``limit`` and ``ef_search`` here are the defaults for any field given as
        a bare query.

        Every field is searched **independently against the same filter** and
        there is no server-side fusion. Merge the lists yourself, or call
        :meth:`SearchResults.rrf` for Reciprocal Rank Fusion.

        For cosine fields, normalize the query the same way the inserts were
        normalized.
        """
        if not fields:
            raise ValidationError("search requires at least one field")

        field_queries = []
        for field_name, spec in fields.items():
            if not isinstance(spec, Query):
                spec = Query(query=spec, limit=limit, ef_search=ef_search)
            field_queries.append(spec.to_json(field_name))

        body: Dict[str, Any] = {"fields": field_queries}
        if filter:
            if not isinstance(filter, Sequence) or isinstance(filter, (str, bytes)):
                raise ValidationError("filter must be a list of conditions")
            body["filter"] = list(filter)

        filter_params = {}
        if prefilter_threshold is not None:
            filter_params["prefilter_threshold"] = prefilter_threshold
        if boost_percentage is not None:
            filter_params["boost_percentage"] = boost_percentage
        if filter_params:
            body["filter_params"] = filter_params

        raw = self._call("search", json.dumps(body).encode())
        return decode_search_result(raw)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class Database:
    """An embedded ndd database rooted at ``data_dir``.

    Open it once and keep it for the process lifetime; it is safe to call from
    many threads (the engine is internally locked). Only one ``data_dir`` may be
    open per process.

    ``db`` is a namespace within the data directory - collections are addressed
    as ``<db>/<collection>``. It carries no authorization meaning in library
    mode; it partitions the collection namespace.

    Usable as a context manager::

        with nddlib.Database("./data") as db:
            ...
    """

    def __init__(
        self,
        data_dir: str,
        db: str = "default",
        *,
        skip_sanity: bool = False,
        save_on_shutdown: bool = True,
        vector_cache_max_bytes: int = 0,
        parallel_insert_threads: int = 0,
    ) -> None:
        self.db = _check_name("db", db)
        self._shared = _acquire_handle(
            data_dir,
            skip_sanity=skip_sanity,
            save_on_shutdown=save_on_shutdown,
            vector_cache_max_bytes=vector_cache_max_bytes,
            parallel_insert_threads=parallel_insert_threads,
        )
        self.data_dir = self._shared.data_dir
        self._closed = False

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        """Release this database. The underlying handle closes once every
        :class:`Database` over the same ``data_dir`` is closed; that flush is
        what persists dirty collections when ``save_on_shutdown`` is set."""
        if self._closed:
            return
        self._closed = True
        _release_handle(self._shared)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<Database {self.data_dir!r} db={self.db!r} {state}>"

    @property
    def version(self) -> str:
        """Engine version and the SIMD ISA it was built for, e.g.
        ``2.1.1+neon``."""
        return _ffi.version()

    def _call(self, op: str, collection_id: Optional[str], request: bytes = b"") -> bytes:
        if self._closed:
            raise NddError("Database is closed")
        return _ffi.call(self._shared.handle, op, collection_id, request)

    # -- collections --------------------------------------------------------

    def create_collection(
        self,
        name: str,
        fields: Sequence[FieldSpec],
        *,
        max_elements: Optional[int] = None,
        size_in_millions: Optional[int] = None,
    ) -> Collection:
        """Create a collection with the given fields and return a handle to it.

        ``fields`` is 1..8 of :class:`~nddlib.VectorField`,
        :class:`~nddlib.MultiVectorField`, or :class:`~nddlib.SparseField`.
        Field geometry is fixed at create time.

        Capacity is provisioned up front: pass ``max_elements``, or
        ``size_in_millions`` (1..10000), which overrides it with N x 1,000,000.
        """
        _check_name("collection", name)
        if not fields:
            raise ValidationError("create_collection requires at least one field")
        if len(fields) > 8:
            raise ValidationError(
                f"a collection may have at most 8 fields, got {len(fields)}"
            )

        body: Dict[str, Any] = {"fields": [f.to_json() for f in fields]}
        if max_elements is not None:
            body["max_elements"] = int(max_elements)
        if size_in_millions is not None:
            if not 1 <= size_in_millions <= 10000:
                raise ValidationError("size_in_millions must be 1..10000")
            body["size_in_millions"] = int(size_in_millions)

        self._call(
            "create_collection", f"{self.db}/{name}", json.dumps(body).encode()
        )
        return Collection(self, name)

    def collection(self, name: str) -> Collection:
        """A handle to an existing collection. This performs no I/O and does not
        check existence - call :meth:`Collection.describe` for that."""
        return Collection(self, name)

    def list_collections(self) -> List[dict]:
        """Every collection in this db, as describe-shaped dicts (without the
        per-field ``element_count``)."""
        raw = self._call("list_collections", self.db)
        return json.loads(raw).get("collections", []) if raw else []

    def collection_names(self) -> List[str]:
        """Just the names of the collections in this db."""
        return [c["name"] for c in self.list_collections()]

    def delete_collection(self, name: str) -> None:
        """Delete a collection and all of its data."""
        _check_name("collection", name)
        self._call("delete_collection", f"{self.db}/{name}")

    def using(self, db: str) -> "Database":
        """Another :class:`Database` over the same ``data_dir`` but a different
        namespace. It shares the underlying handle, so close it when done."""
        return Database(self.data_dir, db)
