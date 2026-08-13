"""Concurrent insert runner with configurable executor backend.

Replaces SerialInsertRunner for faster data loading in performance cases.

Auto-detects thread-unsafe DBs via VectorDB.thread_safe and
falls back to single-worker mode.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import multiprocessing as mp
import os
import threading
import time
from enum import StrEnum
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa

from vectordb_bench.backend.filter import Filter, FilterOp, non_filter
from vectordb_bench.backend.utils import kill_proc_tree, time_it

from ... import config
from ...models import PerformanceTimeoutError
from .executor import AsyncExecutor, ThreadExecutor

if TYPE_CHECKING:
    from vectordb_bench.backend.clients import api
    from vectordb_bench.backend.dataset import DatasetManager

    from .executor import TaskExecutor

log = logging.getLogger(__name__)


class ExecutorBackend(StrEnum):
    THREADING = "threading"
    ASYNC = "async"


class ConcurrentInsertRunner:
    """Concurrent insert runner with pluggable executor backend.

    Thread-safety: If db.thread_safe is False, max_workers is clamped to 1
    so the single worker thread uses self.db directly (no deepcopy needed).

    Args:
        db: VectorDB instance.
        dataset: DatasetManager for batch iteration.
        normalize: Whether to L2-normalize embeddings.
        filters: Filter configuration.
        timeout: Timeout in seconds for the overall operation.
        max_workers: Number of concurrent workers (default: min(cpu_count, 4)).
        backend: Executor backend to use ('threading' or 'async').
    """

    def __init__(
        self,
        db: api.VectorDB,
        dataset: DatasetManager,
        normalize: bool,
        filters: Filter = non_filter,
        timeout: float | None = None,
        max_workers: int | None = None,
        backend: ExecutorBackend = ExecutorBackend.THREADING,
        batch_size: int = config.NUM_PER_BATCH,
        duration: float | None = None,
        with_scalar_labels: bool = False,
        tenant_case=None,  # noqa: ANN001
    ):
        self.timeout = timeout if isinstance(timeout, int | float) else None
        self.dataset: DatasetManager = dataset
        self.db = db
        self.normalize = normalize
        self.filters = filters
        self.backend = backend
        self.batch_size = batch_size
        self.duration = duration if isinstance(duration, int | float) else None
        self.with_scalar_labels = with_scalar_labels
        self.tenant_case = tenant_case

        effective_workers = max_workers or min(mp.cpu_count(), 4)
        if not db.thread_safe:
            log.info(f"DB {db.name} is not thread-safe, falling back to max_workers=1")
            effective_workers = 1
        self.max_workers = effective_workers
        assert db.thread_safe or self.max_workers == 1, (
            "Non-thread-safe DBs must use max_workers=1 — "
            "_get_thread_db() relies on this to avoid concurrent access to self.db"
        )

        # Checkpoint: unique file per collection so parallel runs don't collide
        collection_name = getattr(db, "collection_name", "unknown_index")
        self._checkpoint_path = ConcurrentInsertRunner.checkpoint_path_for(collection_name)
        log.info(f"Checkpoint path: {self._checkpoint_path}")

    def __getstate__(self):
        """Exclude unpicklable thread-local state for ProcessPoolExecutor(spawn)."""
        state = self.__dict__.copy()
        state.pop("_iter_lock", None)
        state.pop("_dataset_iter", None)
        state.pop("_stop_event", None)
        return state

    def _create_executor(self) -> TaskExecutor:
        if self.backend == ExecutorBackend.ASYNC:
            return AsyncExecutor(max_workers=self.max_workers)
        return ThreadExecutor(max_workers=self.max_workers)

    def _get_thread_db(self) -> api.VectorDB:
        """Return self.db.

        All workers share the connection opened by task()'s `with self.db.init()`.
        Thread-safe DBs share it across multiple workers. Non-thread-safe DBs are
        clamped to max_workers=1, so there is never concurrent access.
        """
        return self.db

    def _insert_batch_with_retry(
        self,
        db: api.VectorDB,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        tenant_labels_data: list[str] | None = None,
        extra_fields: dict | None = None,
        retry_idx: int = 0,
    ) -> int:
        """Insert a single batch with retry logic. Returns inserted count."""
        insert_kwargs = {
            "embeddings": embeddings,
            "metadata": metadata,
            "labels_data": labels_data,
        }
        if tenant_labels_data is not None:
            insert_kwargs["tenant_labels_data"] = tenant_labels_data
        if extra_fields:
            insert_kwargs["extra_fields"] = extra_fields
        insert_count, error = db.insert_embeddings(**insert_kwargs)
        if error is not None:
            log.warning(f"Insert failed, try_idx={retry_idx}, Exception: {error}")
            if getattr(error, "non_retryable", False):
                msg = f"Non-retryable insert failure after {insert_count} inserted rows: {error}"
                raise RuntimeError(msg) from error
            retry_idx += 1
            if retry_idx <= config.MAX_INSERT_RETRY:
                time.sleep(retry_idx)
                return self._insert_batch_with_retry(
                    db,
                    embeddings,
                    metadata,
                    labels_data,
                    tenant_labels_data,
                    extra_fields,
                    retry_idx,
                )
            msg = f"Insert failed and retried more than {config.MAX_INSERT_RETRY} times"
            raise RuntimeError(msg)
        return insert_count

    def _worker_insert(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        tenant_labels_data: list[str] | None = None,
        extra_fields: dict | None = None,
    ) -> int:
        """Worker function: insert a batch with retry."""
        db = self._get_thread_db()
        return self._insert_batch_with_retry(db, embeddings, metadata, labels_data, tenant_labels_data, extra_fields)

    def _next_batch(self) -> tuple[list[list[float]], list[int], list[str] | None, list[str] | None, dict | None] | None:
        """Pull the next batch from the shared dataset iterator.

        Thread-safe: only one thread reads from the iterator at a time.
        Returns None when the iterator is exhausted.
        """
        stop_event = getattr(self, "_stop_event", None)
        if stop_event is not None and stop_event.is_set():
            return None
        if self._deadline is not None and time.perf_counter() >= self._deadline:
            return None
        with self._iter_lock:
            stop_event = getattr(self, "_stop_event", None)
            if stop_event is not None and stop_event.is_set():
                return None
            try:
                data_df = next(self._dataset_iter)
            except StopIteration:
                return None

        all_metadata = data_df[self.dataset.data.train_id_field].tolist()
        emb_np = np.stack(data_df[self.dataset.data.train_vector_field])
        if self.normalize:
            all_embeddings = (emb_np / np.linalg.norm(emb_np, axis=1)[:, np.newaxis]).tolist()
        else:
            all_embeddings = emb_np.tolist()
        del emb_np

        labels_data = None
        if self.filters.type == FilterOp.StrEqual or self.with_scalar_labels:
            label_field = self.filters.label_field if self.filters.type == FilterOp.StrEqual else "labels"
            if self.dataset.data.scalar_labels_file_separated:
                labels_data = self.dataset.scalar_labels[label_field][all_metadata].to_list()
            else:
                labels_data = data_df[label_field].tolist()

        tenant_labels_data = None
        if self.tenant_case is not None and getattr(self.tenant_case, "is_multitenant", False):
            tenant_labels_data = self.tenant_case.tenant_labels_for_ids(all_metadata)

        # Extract any extra vector columns (e.g. multivec1, multivec2) that are
        # not the standard id/emb/labels columns so per-field multi-vector clients
        # can use distinct vectors instead of replicating a single embedding.
        _standard = {
            self.dataset.data.train_id_field,
            self.dataset.data.train_vector_field,
            "labels",
            "scalar_labels",
            "tenant_labels",
        }
        extra_fields: dict | None = None
        for col in data_df.columns:
            if col not in _standard:
                sample = data_df[col].iloc[0] if len(data_df) > 0 else None
                if isinstance(sample, (list, np.ndarray)):
                    if extra_fields is None:
                        extra_fields = {}
                    # pandas may return PyArrow-backed scalars for nested list
                    # columns (e.g. list<list<float32>>).  Re-route through PyArrow
                    # to_pylist() which always gives plain Python lists.
                    extra_fields[col] = pa.Array.from_pandas(data_df[col]).to_pylist()

        return all_embeddings, all_metadata, labels_data, tenant_labels_data, extra_fields

    @staticmethod
    def checkpoint_path_for(collection_name: str) -> str:
        safe_name = "".join(c for c in collection_name if c.isalnum() or c in ("_", "-"))
        _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        return os.path.join(_root, f"insert_checkpoint_{safe_name}.json")

    @staticmethod
    def has_checkpoint_for(collection_name: str) -> bool:
        return os.path.exists(ConcurrentInsertRunner.checkpoint_path_for(collection_name))

    def _save_checkpoint(self, count: int) -> None:
        try:
            with open(self._checkpoint_path, "w") as f:
                json.dump({"last_index": count}, f)
        except Exception as e:
            log.warning(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self) -> int:
        if os.path.exists(self._checkpoint_path):
            try:
                with open(self._checkpoint_path) as f:
                    data = json.load(f)
                    return data.get("last_index", 0)
            except Exception as e:
                log.warning(f"Failed to read checkpoint: {e}")
        return 0

    def _clear_checkpoint(self) -> None:
        if os.path.exists(self._checkpoint_path):
            try:
                os.remove(self._checkpoint_path)
                log.info(f"Cleared checkpoint: {self._checkpoint_path}")
            except Exception as e:
                log.warning(f"Failed to clear checkpoint: {e}")

    def _worker_loop(self) -> int:
        """Worker loop: pull batches from the shared iterator and insert them."""
        total = 0
        try:
            while True:
                batch = self._next_batch()
                if batch is None:
                    break
                embeddings, metadata, labels_data, tenant_labels_data, extra_fields = batch
                count = self._worker_insert(embeddings, metadata, labels_data, tenant_labels_data, extra_fields)
                total += count
                # Atomically update shared total and persist checkpoint
                with self._checkpoint_lock:
                    self._total_inserted += count
                    self._save_checkpoint(self._last_index + self._total_inserted)
        except Exception:
            stop_event = getattr(self, "_stop_event", None)
            if stop_event is not None:
                stop_event.set()
            raise
        return total

    def task(self) -> int:
        """Insert entire dataset using concurrent executor. Runs in subprocess."""
        # Resume support: load how many vectors were already inserted
        last_index = self._load_checkpoint()
        if last_index > 0:
            log.info(f"({mp.current_process().name:16}) Resuming from checkpoint: {last_index} vectors already inserted")

        self._last_index = last_index
        self._total_inserted = 0
        self._checkpoint_lock = threading.Lock()
        self._iter_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._deadline = None if self.duration is None else time.perf_counter() + self.duration
        self._dataset_iter = self.dataset.iter_batches(self.batch_size)

        # Skip batches already covered by the checkpoint (single-threaded, before workers start)
        if last_index > 0:
            skipped = 0
            while skipped < last_index:
                try:
                    data_df = next(self._dataset_iter)
                except StopIteration:
                    log.warning("Dataset exhausted while skipping to checkpoint — checkpoint may exceed dataset size")
                    self._clear_checkpoint()
                    return last_index
                batch_size = len(data_df)
                skipped += batch_size
                if skipped % 100_000 < batch_size:
                    log.debug(f"Skipping batches to resume, skipped={skipped}/{last_index}")
            log.info(f"({mp.current_process().name:16}) Skipped {skipped} vectors to resume from checkpoint")

        with self.db.init():
            log.info(
                f"({mp.current_process().name:16}) Start concurrent insert, "
                f"batch_size={self.batch_size}, max_workers={self.max_workers}"
            )
            start = time.perf_counter()

            with self._create_executor() as executor:
                for _ in range(self.max_workers):
                    executor.submit(self._worker_loop)

                batch_results = executor.wait_all()

            # Log all errors, then raise the first one
            errors = [r.error for r in batch_results if r.error is not None]
            if errors:
                for err in errors:
                    log.warning(f"Batch insert error: {err}")
                raise errors[0]

            count_new = sum(r.value for r in batch_results)
            total = last_index + count_new

            log.info(
                f"({mp.current_process().name:16}) Finish concurrent insert, "
                f"new={count_new}, total={total}, dur={time.perf_counter() - start:.2f}s"
            )

        self._clear_checkpoint()
        return total

    @time_it
    def _insert_all_batches(self) -> int:
        """Performance case only: run task() in subprocess with timeout."""
        with concurrent.futures.ProcessPoolExecutor(
            mp_context=mp.get_context("spawn"),
            max_workers=1,
        ) as executor:
            future = executor.submit(self.task)
            try:
                count = future.result(timeout=self.timeout)
            except TimeoutError as e:
                msg = f"VectorDB load dataset timeout in {self.timeout}"
                log.warning(msg)
                kill_proc_tree(pids=list(executor._processes.keys()))
                raise PerformanceTimeoutError(msg) from e
            except Exception as e:
                log.warning(f"VectorDB load dataset error: {e}")
                raise e from e
            else:
                return count

    def run(self) -> int:
        """Insert full dataset concurrently. Returns total inserted count."""
        count, _ = self._insert_all_batches()
        return count
