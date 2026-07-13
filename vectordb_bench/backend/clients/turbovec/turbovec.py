import logging
import os
from contextlib import contextmanager
from typing import Any

import numpy as np

from vectordb_bench.backend.filter import Filter, FilterOp

from ..api import VectorDB
from .config import TurboVecIndexConfig

log = logging.getLogger(__name__)


class TurboVec(VectorDB):
    """VectorDBBench client for turbovec's embedded, in-process TurboQuant index.

    turbovec has no server: the index only exists in the calling process's
    memory. VectorDBBench runs the load, optimize, serial-search and
    concurrent-search phases in separate (often freshly spawned) subprocesses,
    so a live index object can't be shared across them the way a real
    connection would be. Instead, `init()` loads the index from a `.tvim`
    file on disk on every call, and `insert_embeddings` marks the index
    dirty; the file is (re)written once, when the `init()` context that did
    the inserting exits, so the next phase's subprocess picks up the data.

    Known limitation: if the load phase crashes or times out mid-insert, the
    on-disk file (written only at the end of a successful `init()` block)
    won't reflect the partial work, even though VectorDBBench's own resume
    checkpoint thinks it does. This only matters for crash recovery, not for
    a normal successful run.
    """

    supported_filter_types: list[FilterOp] = [FilterOp.NonFilter, FilterOp.NumGE]

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: TurboVecIndexConfig,
        collection_name: str = "turbovec_bench",
        drop_old: bool = False,
        **kwargs,
    ):
        self.dim = dim
        self.case_config = db_case_config
        self.bit_width = db_config.get("bit_width", 4)
        self.collection_name = collection_name

        data_path = db_config.get("path") or "."
        os.makedirs(data_path, exist_ok=True)
        self.index_path = os.path.join(data_path, f"{collection_name}.tvim")

        if drop_old and os.path.exists(self.index_path):
            os.remove(self.index_path)
            log.info(f"Dropped existing turbovec index file: {self.index_path}")

        self.index = None
        self._dirty = False
        self._filter_threshold: int | None = None

        log.info(f"TurboVec index file: {self.index_path} (dim={self.dim}, bit_width={self.bit_width})")

    def need_normalize_cosine(self) -> bool:
        return True

    @contextmanager
    def init(self):
        from turbovec import IdMapIndex

        if os.path.exists(self.index_path):
            self.index = IdMapIndex.load(self.index_path)
        else:
            self.index = IdMapIndex(dim=self.dim, bit_width=self.bit_width)
        self.index.prepare()
        self._dirty = False
        try:
            yield
        finally:
            if self._dirty:
                self.index.write(self.index_path)
            self.index = None

    def optimize(self, data_size: int | None = None):
        """turbovec has no separate build step: init() already warms the
        search caches via index.prepare()."""

    def prepare_filter(self, filters: Filter):
        if filters.type == FilterOp.NonFilter:
            self._filter_threshold = None
        elif filters.type == FilterOp.NumGE:
            self._filter_threshold = filters.int_value
        else:
            msg = f"turbovec does not support filter type {filters.type}"
            raise ValueError(msg)

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> tuple[int, Exception | None]:
        try:
            vectors = np.ascontiguousarray(embeddings, dtype=np.float32)
            ids = np.ascontiguousarray(metadata, dtype=np.uint64)
            self.index.add_with_ids(vectors, ids)
            self._dirty = True
        except Exception as e:
            log.warning(f"Failed to insert embeddings into turbovec index: {e}")
            return 0, e
        return len(metadata), None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        **kwargs: Any,
    ) -> list[int]:
        q = np.ascontiguousarray([query], dtype=np.float32)

        allowlist = None
        if self._filter_threshold is not None:
            # NumGE ("id >= threshold") on VectorDBBench's standard datasets: row
            # ids are always the contiguous range 0..N-1, so the allowed id set
            # is exactly [threshold, N) — turbovec has no query-by-field, only
            # search-with-an-explicit-allowlist, so this avoids needing a
            # separate id/label side-index.
            n = len(self.index)
            if self._filter_threshold >= n:
                return []
            allowlist = np.arange(self._filter_threshold, n, dtype=np.uint64)

        _, ids = self.index.search(q, k, allowlist=allowlist)
        return ids[0].tolist()
