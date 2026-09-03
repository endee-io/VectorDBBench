"""VectorDBBench client for the embedded Endee engine (``nddlib``).

Unlike the ``endee`` client, which talks to an Endee server over HTTP, this one
drives ``libndd_capi`` in-process: no server, no network, no auth token. The
engine allows a single ``data_dir`` per process, so the database handle is
opened inside :meth:`init` (once per benchmark process) rather than being held
on the instance, which also keeps the object picklable for the multiprocess
runners.
"""

import logging
from collections.abc import Iterable
from contextlib import contextmanager

from . import nddlib

from vectordb_bench.backend.filter import Filter, FilterOp

from ..api import VectorDB
from .config import EndeeLibConfig, EndeeLibIndexConfig

log = logging.getLogger(__name__)

_VECTOR_FIELD_NAME = "dense"
_DEFAULT_MAX_ELEMENTS = 1_000_000


class EndeeLib(VectorDB):
    supported_filter_types: list[FilterOp] = [
        FilterOp.NonFilter,
        FilterOp.NumGE,
        FilterOp.StrEqual,
    ]

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: EndeeLibIndexConfig,
        collection_name: str | None = None,
        drop_old: bool = False,
        with_scalar_labels: bool = False,
        **kwargs,
    ):
        self.name = "EndeeLib"
        self.dim = dim
        self.db_config = db_config
        self.case_config = db_case_config
        self.with_scalar_labels = with_scalar_labels

        self.data_dir = db_config.get("data_dir", "./endee_data")
        self.db_namespace = db_config.get("db") or "default"
        self.collection_name = collection_name or db_config.get("collection_name") or "endee_bench"

        self._open_kwargs = {
            "skip_sanity": bool(db_config.get("skip_sanity", False)),
            "save_on_shutdown": bool(db_config.get("save_on_shutdown", True)),
            "vector_cache_max_bytes": int(db_config.get("vector_cache_max_bytes", 0) or 0),
            "parallel_insert_threads": int(db_config.get("parallel_insert_threads", 0) or 0),
        }

        self.index_param = db_case_config.index_param()
        # Resolved once so the search hot path never re-derives it.
        self.search_config = db_case_config.search_param()
        self.ef_search = self.search_config.pop("ef_search", 128)
        self.filter_params = self.search_config

        self._scalar_id_field = "id"
        self._scalar_label_field = "label"
        self.filter_expr = None

        self.db = None
        self.collection = None

        # Provision the collection up front, then release the handle: the engine
        # permits one data_dir per process and the runners re-open it in theirs.
        with self._database() as db:
            if drop_old:
                self._drop_collection(db)
            self._create_collection_if_absent(db)

    # -- lifecycle ----------------------------------------------------------

    @contextmanager
    def _database(self):
        db = nddlib.Database(self.data_dir, self.db_namespace, **self._open_kwargs)
        try:
            yield db
        finally:
            db.close()

    def _drop_collection(self, db: "nddlib.Database"):
        try:
            db.delete_collection(self.collection_name)
            log.info(f"Dropped Endee collection: {self.collection_name}")
        except Exception as e:
            log.warning(f"Failed to drop Endee collection {self.collection_name}: {e}")

    def _create_collection_if_absent(self, db: "nddlib.Database"):
        if self.collection_name in db.collection_names():
            log.info(f"Using existing Endee collection: {self.collection_name}")
            return

        max_elements = self.case_config.max_elements
        size_in_millions = self.case_config.size_in_millions
        if max_elements is None and size_in_millions is None:
            max_elements = _DEFAULT_MAX_ELEMENTS
            log.warning(
                f"Neither max_elements nor size_in_millions was set; provisioning "
                f"{_DEFAULT_MAX_ELEMENTS} elements. Capacity is fixed at create time — "
                f"set --size-in-millions for larger datasets."
            )

        field = nddlib.VectorField(
            _VECTOR_FIELD_NAME,
            dimension=self.dim,
            space_type=self.index_param["space_type"],
            precision=self.index_param["precision"],
            M=self.index_param["M"],
            ef_con=self.index_param["ef_con"],
        )
        db.create_collection(
            self.collection_name,
            fields=[field],
            max_elements=max_elements,
            size_in_millions=size_in_millions,
        )
        log.info(f"Created Endee collection {self.collection_name}: {field}")

    @contextmanager
    def init(self):
        self.db = nddlib.Database(self.data_dir, self.db_namespace, **self._open_kwargs)
        self.collection = self.db.collection(self.collection_name)
        try:
            yield
        finally:
            self.collection = None
            self.db.close()
            self.db = None

    @classmethod
    def config_cls(cls) -> type[EndeeLibConfig]:
        return EndeeLibConfig

    @classmethod
    def case_config_cls(cls, index_type: str | None = None) -> type[EndeeLibIndexConfig]:
        return EndeeLibIndexConfig

    def need_normalize_cosine(self) -> bool:
        """The engine stores no norms, so cosine requires normalized input."""
        return self.index_param["space_type"] == "cosine"

    def optimize(self, data_size: int | None = None):
        """HNSW is built on insert; there is no separate build step."""

    # -- filters ------------------------------------------------------------

    def prepare_filter(self, filters: Filter):
        if filters.type == FilterOp.NonFilter:
            self.filter_expr = None
        elif filters.type == FilterOp.NumGE:
            self.filter_expr = [{self._scalar_id_field: {"$gte": filters.int_value}}]
        elif filters.type == FilterOp.StrEqual:
            self.filter_expr = [{self._scalar_label_field: {"$eq": filters.label_value}}]
        else:
            msg = f"Not support Filter for EndeeLib - {filters}"
            raise ValueError(msg)

    # -- data plane ---------------------------------------------------------

    def insert_embeddings(
        self,
        embeddings: Iterable[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        **kwargs,
    ) -> tuple[int, Exception | None]:
        embeddings = list(embeddings)
        assert len(embeddings) == len(metadata)

        try:
            objects = []
            for i, key in enumerate(metadata):
                key = int(key)
                obj_filter = {self._scalar_id_field: key}
                if self.with_scalar_labels and labels_data is not None:
                    obj_filter[self._scalar_label_field] = labels_data[i]
                objects.append(
                    nddlib.Object(
                        id=str(key),
                        vectors={_VECTOR_FIELD_NAME: embeddings[i]},
                        filter=obj_filter,
                    )
                )
            self.collection.add(objects)
        except Exception as e:
            log.warning(f"Failed to insert data into Endee collection ({self.collection_name}): {e}")
            return 0, e

        return len(embeddings), None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        timeout: int | None = None,
        **kwargs,
    ) -> list[int]:
        results = self.collection.search(
            {_VECTOR_FIELD_NAME: query},
            filter=self.filter_expr,
            limit=k,
            ef_search=self.ef_search,
            **self.filter_params,
        )
        return [int(hit.id) for hit in results.get(_VECTOR_FIELD_NAME, [])]

    def describe_index(self) -> dict:
        try:
            return self.collection.describe()
        except Exception as e:
            log.warning(f"Error describing Endee collection: {e}")
            return {}
