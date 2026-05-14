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

_CURRENT_FILE = os.path.abspath(__file__)
ROOT_BENCHMARK_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_CURRENT_FILE))))


class ExecutorBackend(StrEnum):
    THREADING = "threading"
    ASYNC = "async"


class ConcurrentInsertRunner:
    def __init__(
        self,
        db: api.VectorDB,
        dataset: DatasetManager,
        normalize: bool,
        filters: Filter = non_filter,
        timeout: float | None = None,
        max_workers: int | None = None,
        backend: ExecutorBackend = ExecutorBackend.THREADING,
    ):
        self.timeout = timeout if isinstance(timeout, int | float) else None
        self.dataset: DatasetManager = dataset
        self.db = db
        self.normalize = normalize
        self.filters = filters
        self.backend = backend

        effective_workers = max_workers or min(mp.cpu_count(), 4)
        if not db.thread_safe:
            log.info(f"DB {db.name} is not thread-safe, falling back to max_workers=1")
            effective_workers = 1
        self.max_workers = effective_workers
        assert db.thread_safe or self.max_workers == 1, (
            "Non-thread-safe DBs must use max_workers=1 — "
            "_get_thread_db() relies on this to avoid concurrent access to self.db"
        )

        collection_name = getattr(db, "collection_name", "unknown_index")
        safe_name = "".join(c for c in collection_name if c.isalnum() or c in ("_", "-"))
        self.checkpoint_path = os.path.join(ROOT_BENCHMARK_DIR, f"insert_checkpoint_{safe_name}.json")
        log.info(f"Using checkpoint path: {self.checkpoint_path}")

    def __getstate__(self):
        """Exclude unpicklable thread-local state for ProcessPoolExecutor(spawn)."""
        state = self.__dict__.copy()
        state.pop("_iter_lock", None)
        state.pop("_dataset_iter", None)
        state.pop("_checkpoint_lock", None)
        state.pop("_completed_chunks", None)
        state.pop("_current_file_idx", None)
        state.pop("_checkpoint_frontier", None)
        state.pop("_contiguous_inserted", None)
        return state

    def _create_executor(self) -> TaskExecutor:
        if self.backend == ExecutorBackend.ASYNC:
            return AsyncExecutor(max_workers=self.max_workers)
        return ThreadExecutor(max_workers=self.max_workers)

    def _save_checkpoint(self, vector_count: int) -> None:
        try:
            with open(self.checkpoint_path, "w") as f:
                json.dump({"vectors_inserted": vector_count}, f)
        except Exception as e:
            log.warning(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self) -> int:
        if os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, "r") as f:
                    return json.load(f).get("vectors_inserted", 0)
            except Exception as e:
                log.warning(f"Failed to read checkpoint: {e}")
        return 0

    def _clear_checkpoint(self) -> None:
        if os.path.exists(self.checkpoint_path):
            try:
                os.remove(self.checkpoint_path)
                log.info(f"Cleared checkpoint: {self.checkpoint_path}")
            except Exception as e:
                log.warning(f"Failed to remove checkpoint: {e}")

    def _get_thread_db(self) -> api.VectorDB:
        return self.db

    def _insert_batch_with_retry(
        self,
        db: api.VectorDB,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        retry_idx: int = 0,
    ) -> int:
        insert_count, error = db.insert_embeddings(
            embeddings=embeddings,
            metadata=metadata,
            labels_data=labels_data,
        )
        if error is not None:
            log.warning(f"Insert failed, try_idx={retry_idx}, Exception: {error}")
            retry_idx += 1
            if retry_idx <= config.MAX_INSERT_RETRY:
                time.sleep(retry_idx)
                return self._insert_batch_with_retry(db, embeddings, metadata, labels_data, retry_idx)
            msg = f"Insert failed and retried more than {config.MAX_INSERT_RETRY} times"
            raise RuntimeError(msg)
        return insert_count

    def _worker_insert(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
    ) -> int:
        db = self._get_thread_db()
        return self._insert_batch_with_retry(db, embeddings, metadata, labels_data)

    def _next_batch(self) -> tuple[int, list[list[float]], list[int], list[str] | None] | None:
        with self._iter_lock:
            try:
                data_df = next(self._dataset_iter)
                file_idx = self._current_file_idx
                self._current_file_idx += 1
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
        if self.filters.type == FilterOp.StrEqual:
            if self.dataset.data.scalar_labels_file_separated:
                labels_data = self.dataset.scalar_labels[self.filters.label_field][all_metadata].to_list()
            else:
                labels_data = data_df[self.filters.label_field].tolist()

        return file_idx, all_embeddings, all_metadata, labels_data

    def _worker_loop(self) -> int:
        total = 0
        while True:
            batch = self._next_batch()
            if batch is None:
                break
            file_idx, embeddings, metadata, labels_data = batch
            inserted = self._worker_insert(embeddings, metadata, labels_data)
            total += inserted
            
            with self._checkpoint_lock:
                # Store the exact count of vectors inserted for this chunk index
                self._completed_chunks[file_idx] = inserted
                
                # Only save contiguous progress to avoid missing data if out of order
                while self._checkpoint_frontier in self._completed_chunks:
                    self._contiguous_inserted += self._completed_chunks.pop(self._checkpoint_frontier)
                    self._checkpoint_frontier += 1
                
                self._save_checkpoint(self._contiguous_inserted)
        return total

    def task(self) -> int:
        count = 0
        already_done_vectors = self._load_checkpoint()
        
        # Generator to skip an exact number of vectors, ignoring chunk sizes
        def skip_generator(iterator, skip_count):
            skipped = 0
            for df in iterator:
                df_len = len(df)
                if skipped + df_len <= skip_count:
                    skipped += df_len
                    continue
                elif skipped < skip_count:
                    to_skip = skip_count - skipped
                    skipped = skip_count
                    yield df.iloc[to_skip:] # Slice exactly where we left off
                else:
                    yield df

        self._dataset_iter = skip_generator(iter(self.dataset), already_done_vectors)
        self._iter_lock = threading.Lock()

        if already_done_vectors > 0:
            log.info(f"Resuming from checkpoint: skipping {already_done_vectors} exactly already-inserted vectors")

        self._current_file_idx = 0
        self._checkpoint_frontier = 0
        self._completed_chunks = {}
        self._checkpoint_lock = threading.Lock()
        self._contiguous_inserted = already_done_vectors

        with self.db.init():
            log.info(
                f"({mp.current_process().name:16}) Start concurrent insert, "
                f"batch_size={config.NUM_PER_BATCH}, max_workers={self.max_workers}"
            )
            start = time.perf_counter()

            with self._create_executor() as executor:
                for _ in range(self.max_workers):
                    executor.submit(self._worker_loop)

                batch_results = executor.wait_all()

            errors = [r.error for r in batch_results if r.error is not None]
            if errors:
                for err in errors:
                    log.warning(f"Batch insert error: {err}")
                raise errors[0]

            count = sum(r.value for r in batch_results)

            log.info(
                f"({mp.current_process().name:16}) Finish concurrent insert, "
                f"count={count}, dur={time.perf_counter() - start:.2f}s"
            )
        return count
    
    # @time_it
    # def _insert_all_batches(self) -> int:
    #     """Performance case only: run task() in subprocess with timeout."""
    #     with concurrent.futures.ProcessPoolExecutor(
    #         mp_context=mp.get_context("spawn"),
    #         max_workers=1,
    #     ) as executor:
    #         future = executor.submit(self.task)
    #         try:
    #             count = future.result(timeout=self.timeout)
    #         except TimeoutError as e:
    #             msg = f"VectorDB load dataset timeout in {self.timeout}"
    #             log.warning(msg)
    #             kill_proc_tree(pids=list(executor._processes.keys()))
    #             raise PerformanceTimeoutError(msg) from e
    #         except Exception as e:
    #             log.warning(f"VectorDB load dataset error: {e}")
    #             raise e from e
    #         else:
    #             return count

    @time_it
    def _insert_all_batches(self) -> int:
        with concurrent.futures.ProcessPoolExecutor(
            mp_context=mp.get_context("spawn"),
            max_workers=1,
        ) as executor:
            future = executor.submit(self.task)
            try:
                count = future.result(timeout=None)
            except Exception as e:
                log.warning(f"VectorDB load dataset error: {e}")
                raise e from e
            else:
                return count

    def run(self) -> int:
        count, _ = self._insert_all_batches()
        return count




# # ====================== MODIFIED ==============================

# """Concurrent insert runner with configurable executor backend.

# Replaces SerialInsertRunner for faster data loading in performance cases.

# Auto-detects thread-unsafe DBs via VectorDB.thread_safe and
# falls back to single-worker mode.
# """

# from __future__ import annotations

# import concurrent.futures
# import json
# import logging
# import multiprocessing as mp
# import os
# import threading
# import time
# from enum import StrEnum
# from typing import TYPE_CHECKING

# import numpy as np

# from vectordb_bench.backend.filter import Filter, FilterOp, non_filter
# from vectordb_bench.backend.utils import kill_proc_tree, time_it

# from ... import config
# from ...models import PerformanceTimeoutError
# from .executor import AsyncExecutor, ThreadExecutor

# if TYPE_CHECKING:
#     from vectordb_bench.backend.clients import api
#     from vectordb_bench.backend.dataset import DatasetManager

#     from .executor import TaskExecutor

# log = logging.getLogger(__name__)

# _CURRENT_FILE = os.path.abspath(__file__)
# ROOT_BENCHMARK_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_CURRENT_FILE))))


# class ExecutorBackend(StrEnum):
#     THREADING = "threading"
#     ASYNC = "async"


# class ConcurrentInsertRunner:
#     """Concurrent insert runner with pluggable executor backend.

#     Thread-safety: If db.thread_safe is False, max_workers is clamped to 1
#     so the single worker thread uses self.db directly (no deepcopy needed).

#     Args:
#         db: VectorDB instance.
#         dataset: DatasetManager for batch iteration.
#         normalize: Whether to L2-normalize embeddings.
#         filters: Filter configuration.
#         timeout: Timeout in seconds for the overall operation.
#         max_workers: Number of concurrent workers (default: min(cpu_count, 4)).
#         backend: Executor backend to use ('threading' or 'async').
#     """

#     def __init__(
#         self,
#         db: api.VectorDB,
#         dataset: DatasetManager,
#         normalize: bool,
#         filters: Filter = non_filter,
#         timeout: float | None = None,
#         max_workers: int | None = None,
#         backend: ExecutorBackend = ExecutorBackend.THREADING,
#     ):
#         self.timeout = timeout if isinstance(timeout, int | float) else None
#         self.dataset: DatasetManager = dataset
#         self.db = db
#         self.normalize = normalize
#         self.filters = filters
#         self.backend = backend

#         effective_workers = max_workers or min(mp.cpu_count(), 4)
#         if not db.thread_safe:
#             log.info(f"DB {db.name} is not thread-safe, falling back to max_workers=1")
#             effective_workers = 1
#         self.max_workers = effective_workers
#         assert db.thread_safe or self.max_workers == 1, (
#             "Non-thread-safe DBs must use max_workers=1 — "
#             "_get_thread_db() relies on this to avoid concurrent access to self.db"
#         )

#         collection_name = getattr(db, "collection_name", "unknown_index")
#         safe_name = "".join(c for c in collection_name if c.isalnum() or c in ("_", "-"))
#         self.checkpoint_path = os.path.join(ROOT_BENCHMARK_DIR, f"insert_checkpoint_{safe_name}.json")
#         log.info(f"Using checkpoint path: {self.checkpoint_path}")

#     def __getstate__(self):
#         """Exclude unpicklable thread-local state for ProcessPoolExecutor(spawn)."""
#         state = self.__dict__.copy()
#         state.pop("_iter_lock", None)
#         state.pop("_dataset_iter", None)
#         state.pop("_checkpoint_lock", None)
#         state.pop("_completed_files", None)
#         state.pop("_current_file_idx", None)
#         state.pop("_checkpoint_frontier", None)
#         return state

#     def _create_executor(self) -> TaskExecutor:
#         if self.backend == ExecutorBackend.ASYNC:
#             return AsyncExecutor(max_workers=self.max_workers)
#         return ThreadExecutor(max_workers=self.max_workers)

#     def _save_checkpoint(self, file_count: int, vector_count: int) -> None:
#         try:
#             with open(self.checkpoint_path, "w") as f:
#                 json.dump({"last_index": file_count, "vectors_inserted": vector_count}, f)
#         except Exception as e:
#             log.warning(f"Failed to save checkpoint: {e}")

#     def _load_checkpoint(self) -> int:
#         if os.path.exists(self.checkpoint_path):
#             try:
#                 with open(self.checkpoint_path, "r") as f:
#                     return json.load(f).get("last_index", 0)
#             except Exception as e:
#                 log.warning(f"Failed to read checkpoint: {e}")
#         return 0

#     def _clear_checkpoint(self) -> None:
#         if os.path.exists(self.checkpoint_path):
#             try:
#                 os.remove(self.checkpoint_path)
#                 log.info(f"Cleared checkpoint: {self.checkpoint_path}")
#             except Exception as e:
#                 log.warning(f"Failed to remove checkpoint: {e}")

#     def _get_thread_db(self) -> api.VectorDB:
#         """Return self.db.

#         All workers share the connection opened by task()'s `with self.db.init()`.
#         Thread-safe DBs share it across multiple workers. Non-thread-safe DBs are
#         clamped to max_workers=1, so there is never concurrent access.
#         """
#         return self.db

#     def _insert_batch_with_retry(
#         self,
#         db: api.VectorDB,
#         embeddings: list[list[float]],
#         metadata: list[int],
#         labels_data: list[str] | None = None,
#         retry_idx: int = 0,
#     ) -> int:
#         """Insert a single batch with retry logic. Returns inserted count."""
#         insert_count, error = db.insert_embeddings(
#             embeddings=embeddings,
#             metadata=metadata,
#             labels_data=labels_data,
#         )
#         if error is not None:
#             log.warning(f"Insert failed, try_idx={retry_idx}, Exception: {error}")
#             retry_idx += 1
#             if retry_idx <= config.MAX_INSERT_RETRY:
#                 time.sleep(retry_idx)
#                 return self._insert_batch_with_retry(db, embeddings, metadata, labels_data, retry_idx)
#             msg = f"Insert failed and retried more than {config.MAX_INSERT_RETRY} times"
#             raise RuntimeError(msg)
#         return insert_count

#     def _worker_insert(
#         self,
#         embeddings: list[list[float]],
#         metadata: list[int],
#         labels_data: list[str] | None = None,
#     ) -> int:
#         """Worker function: insert a batch with retry."""
#         db = self._get_thread_db()
#         return self._insert_batch_with_retry(db, embeddings, metadata, labels_data)

#     def _next_batch(self) -> tuple[int, list[list[float]], list[int], list[str] | None] | None:
#         """Pull the next batch from the shared dataset iterator.

#         Thread-safe: only one thread reads from the iterator at a time.
#         Returns (file_idx, embeddings, metadata, labels_data) or None when exhausted.
#         """
#         with self._iter_lock:
#             try:
#                 data_df = next(self._dataset_iter)
#                 file_idx = self._current_file_idx
#                 self._current_file_idx += 1
#             except StopIteration:
#                 return None

#         all_metadata = data_df[self.dataset.data.train_id_field].tolist()
#         emb_np = np.stack(data_df[self.dataset.data.train_vector_field])
#         if self.normalize:
#             all_embeddings = (emb_np / np.linalg.norm(emb_np, axis=1)[:, np.newaxis]).tolist()
#         else:
#             all_embeddings = emb_np.tolist()
#         del emb_np

#         labels_data = None
#         if self.filters.type == FilterOp.StrEqual:
#             if self.dataset.data.scalar_labels_file_separated:
#                 labels_data = self.dataset.scalar_labels[self.filters.label_field][all_metadata].to_list()
#             else:
#                 labels_data = data_df[self.filters.label_field].tolist()

#         return file_idx, all_embeddings, all_metadata, labels_data

#     def _worker_loop(self) -> int:
#         """Worker loop: pull batches from the shared iterator and insert them."""
#         total = 0
#         while True:
#             batch = self._next_batch()
#             if batch is None:
#                 break
#             file_idx, embeddings, metadata, labels_data = batch
#             inserted = self._worker_insert(embeddings, metadata, labels_data)
#             total += inserted
#             with self._checkpoint_lock:
#                 self._inserted_count += inserted
#                 self._completed_files.add(file_idx)
#                 while self._checkpoint_frontier in self._completed_files:
#                     self._checkpoint_frontier += 1
#                 self._save_checkpoint(self._checkpoint_frontier, self._inserted_count)
#         return total

#     def task(self) -> int:
#         """Insert entire dataset using concurrent executor. Runs in subprocess."""
#         count = 0
#         self._iter_lock = threading.Lock()
#         self._dataset_iter = iter(self.dataset)

#         already_done = self._load_checkpoint()
#         if already_done > 0:
#             log.info(f"Resuming from checkpoint: skipping {already_done} already-inserted files")
#             for _ in range(already_done):
#                 try:
#                     next(self._dataset_iter)
#                 except StopIteration:
#                     break

#         self._current_file_idx = already_done
#         self._checkpoint_frontier = already_done
#         self._completed_files: set[int] = set()
#         self._checkpoint_lock = threading.Lock()
#         self._inserted_count = 0

#         with self.db.init():
#             log.info(
#                 f"({mp.current_process().name:16}) Start concurrent insert, "
#                 f"batch_size={config.NUM_PER_BATCH}, max_workers={self.max_workers}"
#             )
#             start = time.perf_counter()

#             with self._create_executor() as executor:
#                 for _ in range(self.max_workers):
#                     executor.submit(self._worker_loop)

#                 batch_results = executor.wait_all()

#             # Log all errors, then raise the first one
#             errors = [r.error for r in batch_results if r.error is not None]
#             if errors:
#                 for err in errors:
#                     log.warning(f"Batch insert error: {err}")
#                 raise errors[0]

#             count = sum(r.value for r in batch_results)

#             log.info(
#                 f"({mp.current_process().name:16}) Finish concurrent insert, "
#                 f"count={count}, dur={time.perf_counter() - start:.2f}s"
#             )
#         return count

#     # @time_it
#     # def _insert_all_batches(self) -> int:
#     #     """Performance case only: run task() in subprocess with timeout."""
#     #     with concurrent.futures.ProcessPoolExecutor(
#     #         mp_context=mp.get_context("spawn"),
#     #         max_workers=1,
#     #     ) as executor:
#     #         future = executor.submit(self.task)
#     #         try:
#     #             count = future.result(timeout=self.timeout)
#     #         except TimeoutError as e:
#     #             msg = f"VectorDB load dataset timeout in {self.timeout}"
#     #             log.warning(msg)
#     #             kill_proc_tree(pids=list(executor._processes.keys()))
#     #             raise PerformanceTimeoutError(msg) from e
#     #         except Exception as e:
#     #             log.warning(f"VectorDB load dataset error: {e}")
#     #             raise e from e
#     #         else:
#     #             return count

#     @time_it
#     def _insert_all_batches(self) -> int:
#         """Performance case only: run task() in subprocess without timeout."""
#         with concurrent.futures.ProcessPoolExecutor(
#             mp_context=mp.get_context("spawn"),
#             max_workers=1,
#         ) as executor:
#             future = executor.submit(self.task)
#             try:
#                 # timeout=None forces the main process to wait indefinitely until insertion finishes
#                 count = future.result(timeout=None)
#             except Exception as e:
#                 log.warning(f"VectorDB load dataset error: {e}")
#                 raise e from e
#             else:
#                 return count

#     def run(self) -> int:
#         """Insert full dataset concurrently. Returns total inserted count."""
#         count, _ = self._insert_all_batches()
#         return count



# ======================= ORIGINAL =================================

# """Concurrent insert runner with configurable executor backend.

# Replaces SerialInsertRunner for faster data loading in performance cases.

# Auto-detects thread-unsafe DBs via VectorDB.thread_safe and
# falls back to single-worker mode.
# """

# from __future__ import annotations

# import concurrent.futures
# import logging
# import multiprocessing as mp
# import threading
# import time
# from enum import StrEnum
# from typing import TYPE_CHECKING

# import numpy as np

# from vectordb_bench.backend.filter import Filter, FilterOp, non_filter
# from vectordb_bench.backend.utils import kill_proc_tree, time_it

# from ... import config
# from ...models import PerformanceTimeoutError
# from .executor import AsyncExecutor, ThreadExecutor

# if TYPE_CHECKING:
#     from vectordb_bench.backend.clients import api
#     from vectordb_bench.backend.dataset import DatasetManager

#     from .executor import TaskExecutor

# log = logging.getLogger(__name__)


# class ExecutorBackend(StrEnum):
#     THREADING = "threading"
#     ASYNC = "async"


# class ConcurrentInsertRunner:
#     """Concurrent insert runner with pluggable executor backend.

#     Thread-safety: If db.thread_safe is False, max_workers is clamped to 1
#     so the single worker thread uses self.db directly (no deepcopy needed).

#     Args:
#         db: VectorDB instance.
#         dataset: DatasetManager for batch iteration.
#         normalize: Whether to L2-normalize embeddings.
#         filters: Filter configuration.
#         timeout: Timeout in seconds for the overall operation.
#         max_workers: Number of concurrent workers (default: min(cpu_count, 4)).
#         backend: Executor backend to use ('threading' or 'async').
#     """

#     def __init__(
#         self,
#         db: api.VectorDB,
#         dataset: DatasetManager,
#         normalize: bool,
#         filters: Filter = non_filter,
#         timeout: float | None = None,
#         max_workers: int | None = None,
#         backend: ExecutorBackend = ExecutorBackend.THREADING,
#     ):
#         self.timeout = timeout if isinstance(timeout, int | float) else None
#         self.dataset: DatasetManager = dataset
#         self.db = db
#         self.normalize = normalize
#         self.filters = filters
#         self.backend = backend

#         effective_workers = max_workers or min(mp.cpu_count(), 4)
#         if not db.thread_safe:
#             log.info(f"DB {db.name} is not thread-safe, falling back to max_workers=1")
#             effective_workers = 1
#         self.max_workers = effective_workers
#         assert db.thread_safe or self.max_workers == 1, (
#             "Non-thread-safe DBs must use max_workers=1 — "
#             "_get_thread_db() relies on this to avoid concurrent access to self.db"
#         )

#     def __getstate__(self):
#         """Exclude unpicklable thread-local state for ProcessPoolExecutor(spawn)."""
#         state = self.__dict__.copy()
#         state.pop("_iter_lock", None)
#         state.pop("_dataset_iter", None)
#         return state

#     def _create_executor(self) -> TaskExecutor:
#         if self.backend == ExecutorBackend.ASYNC:
#             return AsyncExecutor(max_workers=self.max_workers)
#         return ThreadExecutor(max_workers=self.max_workers)

#     def _get_thread_db(self) -> api.VectorDB:
#         """Return self.db.

#         All workers share the connection opened by task()'s `with self.db.init()`.
#         Thread-safe DBs share it across multiple workers. Non-thread-safe DBs are
#         clamped to max_workers=1, so there is never concurrent access.
#         """
#         return self.db

#     def _insert_batch_with_retry(
#         self,
#         db: api.VectorDB,
#         embeddings: list[list[float]],
#         metadata: list[int],
#         labels_data: list[str] | None = None,
#         retry_idx: int = 0,
#     ) -> int:
#         """Insert a single batch with retry logic. Returns inserted count."""
#         insert_count, error = db.insert_embeddings(
#             embeddings=embeddings,
#             metadata=metadata,
#             labels_data=labels_data,
#         )
#         if error is not None:
#             log.warning(f"Insert failed, try_idx={retry_idx}, Exception: {error}")
#             retry_idx += 1
#             if retry_idx <= config.MAX_INSERT_RETRY:
#                 time.sleep(retry_idx)
#                 return self._insert_batch_with_retry(db, embeddings, metadata, labels_data, retry_idx)
#             msg = f"Insert failed and retried more than {config.MAX_INSERT_RETRY} times"
#             raise RuntimeError(msg)
#         return insert_count

#     def _worker_insert(
#         self,
#         embeddings: list[list[float]],
#         metadata: list[int],
#         labels_data: list[str] | None = None,
#     ) -> int:
#         """Worker function: insert a batch with retry."""
#         db = self._get_thread_db()
#         return self._insert_batch_with_retry(db, embeddings, metadata, labels_data)

#     def _next_batch(self) -> tuple[list[list[float]], list[int], list[str] | None] | None:
#         """Pull the next batch from the shared dataset iterator.

#         Thread-safe: only one thread reads from the iterator at a time.
#         Returns None when the iterator is exhausted.
#         """
#         with self._iter_lock:
#             try:
#                 data_df = next(self._dataset_iter)
#             except StopIteration:
#                 return None

#         all_metadata = data_df[self.dataset.data.train_id_field].tolist()
#         emb_np = np.stack(data_df[self.dataset.data.train_vector_field])
#         if self.normalize:
#             all_embeddings = (emb_np / np.linalg.norm(emb_np, axis=1)[:, np.newaxis]).tolist()
#         else:
#             all_embeddings = emb_np.tolist()
#         del emb_np

#         labels_data = None
#         if self.filters.type == FilterOp.StrEqual:
#             if self.dataset.data.scalar_labels_file_separated:
#                 labels_data = self.dataset.scalar_labels[self.filters.label_field][all_metadata].to_list()
#             else:
#                 labels_data = data_df[self.filters.label_field].tolist()

#         return all_embeddings, all_metadata, labels_data

#     def _worker_loop(self) -> int:
#         """Worker loop: pull batches from the shared iterator and insert them."""
#         total = 0
#         while True:
#             batch = self._next_batch()
#             if batch is None:
#                 break
#             embeddings, metadata, labels_data = batch
#             total += self._worker_insert(embeddings, metadata, labels_data)
#         return total

#     def task(self) -> int:
#         """Insert entire dataset using concurrent executor. Runs in subprocess."""
#         count = 0
#         self._iter_lock = threading.Lock()
#         self._dataset_iter = iter(self.dataset)

#         with self.db.init():
#             log.info(
#                 f"({mp.current_process().name:16}) Start concurrent insert, "
#                 f"batch_size={config.NUM_PER_BATCH}, max_workers={self.max_workers}"
#             )
#             start = time.perf_counter()

#             with self._create_executor() as executor:
#                 for _ in range(self.max_workers):
#                     executor.submit(self._worker_loop)

#                 batch_results = executor.wait_all()

#             # Log all errors, then raise the first one
#             errors = [r.error for r in batch_results if r.error is not None]
#             if errors:
#                 for err in errors:
#                     log.warning(f"Batch insert error: {err}")
#                 raise errors[0]

#             count = sum(r.value for r in batch_results)

#             log.info(
#                 f"({mp.current_process().name:16}) Finish concurrent insert, "
#                 f"count={count}, dur={time.perf_counter() - start:.2f}s"
#             )
#         return count

#     @time_it
#     def _insert_all_batches(self) -> int:
#         """Performance case only: run task() in subprocess with timeout."""
#         with concurrent.futures.ProcessPoolExecutor(
#             mp_context=mp.get_context("spawn"),
#             max_workers=1,
#         ) as executor:
#             future = executor.submit(self.task)
#             try:
#                 count = future.result(timeout=self.timeout)
#             except TimeoutError as e:
#                 msg = f"VectorDB load dataset timeout in {self.timeout}"
#                 log.warning(msg)
#                 kill_proc_tree(pids=list(executor._processes.keys()))
#                 raise PerformanceTimeoutError(msg) from e
#             except Exception as e:
#                 log.warning(f"VectorDB load dataset error: {e}")
#                 raise e from e
#             else:
#                 return count

#     def run(self) -> int:
#         """Insert full dataset concurrently. Returns total inserted count."""
#         count, _ = self._insert_all_batches()
#         return count
