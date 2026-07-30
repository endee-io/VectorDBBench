import concurrent.futures
import logging
import math
import multiprocessing as mp
import os
import random
import time
import traceback

import numpy as np

from vectordb_bench.backend.dataset import DatasetManager
from vectordb_bench.backend.filter import Filter, non_filter
from vectordb_bench.backend.payload import PayloadProfile

from ... import config
from ...metric import calc_ndcg, calc_recall, get_ideal_dcg
from ...models import LoadTimeoutError
from .. import utils
from ..clients import api

NUM_PER_BATCH = config.NUM_PER_BATCH
LOAD_MAX_TRY_COUNT = config.LOAD_MAX_TRY_COUNT

log = logging.getLogger(__name__)

# Repo root (3 levels up from vectordb_bench/backend/runner/), same convention
# used by ConcurrentInsertRunner's checkpoint path.
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
SEARCH_RESULTS_DIR = os.path.join(_ROOT_DIR, "search_results")


def _safe_name(name: str) -> str:
    return "".join(c for c in str(name) if c.isalnum() or c in ("_", "-")) or "unknown"


class SerialInsertRunner:
    def __init__(
        self,
        db: api.VectorDB,
        dataset: DatasetManager,
        normalize: bool,
        filters: Filter = non_filter,
        timeout: float | None = None,
    ):
        self.timeout = timeout if isinstance(timeout, int | float) else None
        self.dataset = dataset
        self.db = db
        self.normalize = normalize
        self.filters = filters

    def endless_insert_data(self, all_embeddings: list, all_metadata: list, left_id: int = 0) -> int:
        with self.db.init():
            # unique id for endlessness insertion
            all_metadata = [i + left_id for i in all_metadata]

            num_batches = math.ceil(len(all_embeddings) / NUM_PER_BATCH)
            log.info(
                f"({mp.current_process().name:16}) Start inserting {len(all_embeddings)} "
                f"embeddings in batch {NUM_PER_BATCH}"
            )
            count = 0
            for batch_id in range(num_batches):
                retry_count = 0
                already_insert_count = 0
                metadata = all_metadata[batch_id * NUM_PER_BATCH : (batch_id + 1) * NUM_PER_BATCH]
                embeddings = all_embeddings[batch_id * NUM_PER_BATCH : (batch_id + 1) * NUM_PER_BATCH]

                log.debug(
                    f"({mp.current_process().name:16}) batch [{batch_id:3}/{num_batches}], "
                    f"Start inserting {len(metadata)} embeddings"
                )
                while retry_count < LOAD_MAX_TRY_COUNT:
                    insert_count, error = self.db.insert_embeddings(
                        embeddings=embeddings[already_insert_count:],
                        metadata=metadata[already_insert_count:],
                    )
                    already_insert_count += insert_count
                    if error is not None:
                        retry_count += 1
                        time.sleep(10)

                        log.info(f"Failed to insert data, try {retry_count} time")
                        if retry_count >= LOAD_MAX_TRY_COUNT:
                            raise error
                    else:
                        break
                log.debug(
                    f"({mp.current_process().name:16}) batch [{batch_id:3}/{num_batches}], "
                    f"Finish inserting {len(metadata)} embeddings"
                )

                assert already_insert_count == len(metadata)
                count += already_insert_count
            log.info(
                f"({mp.current_process().name:16}) Finish inserting {len(all_embeddings)} embeddings in "
                f"batch {NUM_PER_BATCH}"
            )
        return count

    def run_endlessness(self) -> int:
        """run forever util DB raises exception or crash"""
        # datasets for load tests are quite small, can fit into memory
        # only 1 file
        data_df = next(iter(self.dataset))
        all_embeddings, all_metadata = (
            np.stack(data_df[self.dataset.data.train_vector_field]).tolist(),
            data_df[self.dataset.data.train_id_field].tolist(),
        )

        start_time = time.perf_counter()
        max_load_count, times = 0, 0
        try:
            while time.perf_counter() - start_time < self.timeout:
                count = self.endless_insert_data(
                    all_embeddings,
                    all_metadata,
                    left_id=max_load_count,
                )
                max_load_count += count
                times += 1
                log.info(
                    f"Loaded {times} entire dataset, current max load counts={utils.numerize(max_load_count)}, "
                    f"{max_load_count}"
                )
        except Exception as e:
            log.info(
                f"Capacity case load reach limit, insertion counts={utils.numerize(max_load_count)}, "
                f"{max_load_count}, err={e}"
            )
            traceback.print_exc()
            return max_load_count
        else:
            raise LoadTimeoutError(self.timeout)


class SerialSearchRunner:
    def __init__(
        self,
        db: api.VectorDB,
        test_data: list[list[float]],
        ground_truth: list[list[int]],
        k: int = 100,
        filters: Filter = non_filter,
        payload_profile: PayloadProfile = PayloadProfile.IDS_ONLY,
        tenant_labels: list[str] | None = None,
        measure_recall: bool = True,
        dump_results: bool = True,
    ):
        self.db = db
        self.k = k
        self.filters = filters
        self.payload_profile = payload_profile
        self.tenant_labels = tenant_labels or []
        self.measure_recall = measure_recall
        self.dump_results = dump_results
        if not self.db.supports_payload_profile(self.payload_profile):
            msg = f"{self.db.name} does not support payload_profile={self.payload_profile.value}"
            raise NotImplementedError(msg)

        if isinstance(test_data[0], np.ndarray):
            self.test_data = [query.tolist() for query in test_data]
        else:
            self.test_data = test_data
        self.ground_truth = ground_truth

    def _search_embedding(self, emb: list[float], tenant: str | None = None) -> list[int]:
        if tenant is None:
            if self.payload_profile == PayloadProfile.IDS_ONLY:
                return self.db.search_embedding(emb, self.k)
            return self.db.search_embedding(emb, self.k, payload_profile=self.payload_profile)
        if self.payload_profile == PayloadProfile.IDS_ONLY:
            return self.db.search_embedding(emb, self.k, tenant=tenant)
        return self.db.search_embedding(emb, self.k, payload_profile=self.payload_profile, tenant=tenant)

    def _get_db_search_res(self, emb: list[float], tenant: str | None = None, retry_idx: int = 0) -> list[int]:
        try:
            results = self._search_embedding(emb, tenant=tenant)
        except Exception as e:
            log.warning(f"Serial search failed, retry_idx={retry_idx}, Exception: {e}")
            if retry_idx < config.MAX_SEARCH_RETRY:
                return self._get_db_search_res(emb=emb, tenant=tenant, retry_idx=retry_idx + 1)

            msg = f"Serial search failed and retried more than {config.MAX_SEARCH_RETRY} times"
            raise RuntimeError(msg) from e

        return results

    def _result_file_path(self) -> str:
        os.makedirs(SEARCH_RESULTS_DIR, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        db_name = _safe_name(self.db.__class__.__name__)
        return os.path.join(SEARCH_RESULTS_DIR, f"{db_name}_{timestamp}.csv")

    def search(self, args: tuple[list, list[list[int]]]) -> tuple[float, float, float, float]:
        log.info(f"{mp.current_process().name:14} start search the entire test_data to get recall and latency")
        with self.db.init():
            self.db.prepare_filter(self.filters)
            test_data, ground_truth = args
            ideal_dcg = get_ideal_dcg(self.k)

            log.debug(f"test dataset size: {len(test_data)}")
            log.debug(f"ground truth size: {len(ground_truth) if ground_truth is not None else 0}")

            latencies, recalls, ndcgs = [], [], []
            missing_counts: list[tuple[int, int]] = []  # (missing_count, query_id), only for queries with gt
            tenant_rng = random.Random(0)

            result_file = self._result_file_path() if self.dump_results else None
            result_fh = None
            if result_file:
                try:
                    result_fh = open(result_file, "w")
                    log.info(f"Dumping per-query search results to {result_file}")
                    result_fh.write(
                        "query_id|returned_ids|ground_truth_ids|missing_ids|missing_count|extra_ids|"
                        "recall|ndcg|latency_ms\n"
                    )
                except OSError as e:
                    log.warning(f"Failed to open result dump file {result_file}, continuing without it: {e}")
                    result_fh = None

            try:
                for idx, emb in enumerate(test_data):
                    tenant = (
                        self.tenant_labels[tenant_rng.randrange(len(self.tenant_labels))]
                        if self.tenant_labels
                        else None
                    )
                    s = time.perf_counter()
                    try:
                        results = self._get_db_search_res(emb, tenant=tenant)
                    except Exception as e:
                        log.warning(f"VectorDB search_embedding error: {e}")
                        raise e from None

                    latency = time.perf_counter() - s
                    latencies.append(latency)

                    gt = ground_truth[idx] if ground_truth is not None else None
                    if self.measure_recall and gt is not None:
                        recall = calc_recall(self.k, gt[: self.k], results)
                        ndcg = calc_ndcg(gt[: self.k], results, ideal_dcg)
                        recalls.append(recall)
                        ndcgs.append(ndcg)
                    else:
                        recall = ndcg = 0
                        recalls.append(0)
                        ndcgs.append(0)

                    returned = results[: self.k]
                    if gt is not None:
                        gt_topk = gt[: self.k]
                        missing = [i for i in gt_topk if i not in returned]
                        extra = [i for i in returned if i not in gt_topk]
                        missing_counts.append((len(missing), idx))
                    else:
                        gt_topk = missing = extra = []

                    if result_fh:
                        try:
                            returned_ids = ",".join(map(str, returned))
                            gt_ids = ",".join(map(str, gt_topk))
                            missing_ids = ",".join(map(str, missing))
                            extra_ids = ",".join(map(str, extra))
                            result_fh.write(
                                f"{idx}|{returned_ids}|{gt_ids}|{missing_ids}|{len(missing)}|{extra_ids}|"
                                f"{recall:.4f}|{ndcg:.4f}|{latency * 1000:.2f}\n"
                            )
                        except OSError as e:
                            log.warning(f"Failed to write result dump row {idx}, disabling further dumps: {e}")
                            result_fh.close()
                            result_fh = None

                    if len(latencies) % 100 == 0:
                        log.debug(
                            f"({mp.current_process().name:14}) search_count={len(latencies):3}, "
                            f"latest_latency={latencies[-1]}, latest recall={recalls[-1]}"
                        )
            finally:
                if result_fh:
                    result_fh.close()

        avg_latency = round(np.mean(latencies), 4)
        avg_recall = round(np.mean(recalls), 4)
        avg_ndcg = round(np.mean(ndcgs), 4)
        cost = round(np.sum(latencies), 4)
        p99 = round(np.percentile(latencies, 99), 4)
        p95 = round(np.percentile(latencies, 95), 4)
        log.info(
            f"{mp.current_process().name:14} search entire test_data: "
            f"cost={cost}s, "
            f"queries={len(latencies)}, "
            f"avg_recall={avg_recall}, "
            f"avg_ndcg={avg_ndcg}, "
            f"avg_latency={avg_latency}, "
            f"p99={p99}, "
            f"p95={p95}"
        )

        if self.dump_results and result_file:
            self._write_summary_file(
                result_file=result_file,
                num_queries=len(latencies),
                avg_latency=avg_latency,
                avg_recall=avg_recall,
                avg_ndcg=avg_ndcg,
                cost=cost,
                p99=p99,
                p95=p95,
                recalls=recalls,
                missing_counts=missing_counts,
                measured=self.measure_recall and ground_truth is not None,
            )

        return (avg_recall, avg_ndcg, p99, p95)

    def _write_summary_file(
        self,
        result_file: str,
        num_queries: int,
        avg_latency: float,
        avg_recall: float,
        avg_ndcg: float,
        cost: float,
        p99: float,
        p95: float,
        recalls: list[float],
        missing_counts: list[tuple[int, int]],
        measured: bool,
    ) -> None:
        summary_file = result_file[: -len(".csv")] + ".txt" if result_file.endswith(".csv") else result_file + ".txt"
        try:
            lines = [
                "=== Run info ===",
                f"db: {self.db.__class__.__name__}",
                f"collection: {getattr(self.db, 'collection_name', 'unknown')}",
                f"k: {self.k}",
                f"filter_type: {self.filters.type.value}",
            ]
            if hasattr(self.filters, "label_percentage"):
                lines.append(f"label_percentage: {self.filters.label_percentage * 100:.2f}%")
            elif self.filters.filter_rate:
                lines.append(f"filter_rate: {self.filters.filter_rate * 100:.2f}%")
            lines.append(f"payload_profile: {self.payload_profile.value}")
            if self.tenant_labels:
                lines.append(f"tenant_labels: {len(self.tenant_labels)}")
            lines.append(f"measure_recall: {measured}")
            lines.append(f"queries: {num_queries}")
            lines.append(f"result_csv: {os.path.basename(result_file)}")

            lines += [
                "",
                "=== Aggregate metrics ===",
                f"avg_recall: {avg_recall}",
                f"avg_ndcg: {avg_ndcg}",
                f"avg_latency_s: {avg_latency}",
                f"p95_latency_s: {p95}",
                f"p99_latency_s: {p99}",
                f"total_cost_s: {cost}",
            ]

            if measured and recalls:
                perfect = sum(1 for r in recalls if r >= 1.0)
                zero = sum(1 for r in recalls if r <= 0.0)
                lines += [
                    "",
                    "=== Recall distribution ===",
                    f"min_recall: {round(min(recalls), 4)}",
                    f"max_recall: {round(max(recalls), 4)}",
                    f"queries_with_perfect_recall: {perfect}/{num_queries}",
                    f"queries_with_zero_recall: {zero}/{num_queries}",
                ]

            if missing_counts:
                total_missing = sum(c for c, _ in missing_counts)
                worst = sorted(missing_counts, reverse=True)[:10]
                lines += [
                    "",
                    "=== Missing-id analysis (evidence of accuracy issues) ===",
                    f"total_missing_ids_across_all_queries: {total_missing}",
                    f"avg_missing_ids_per_query: {round(total_missing / len(missing_counts), 2)}",
                    f"max_missing_ids_in_a_single_query: {worst[0][0] if worst else 0}",
                    "worst_queries_by_missing_count (query_id: missing_count):",
                ]
                lines += [f"  {qid}: {cnt}" for cnt, qid in worst]

            with open(summary_file, "w") as f:
                f.write("\n".join(lines) + "\n")
            log.info(f"Wrote run summary to {summary_file}")
        except OSError as e:
            log.warning(f"Failed to write summary file {summary_file}: {e}")

    def _run_in_subprocess(self) -> tuple[float, float, float, float]:
        with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.search, (self.test_data, self.ground_truth))
            return future.result()

    @utils.time_it
    def run(self) -> tuple[float, float, float, float]:
        log.info(f"{mp.current_process().name:14} start serial search")
        if self.test_data is None:
            msg = "empty test_data"
            raise RuntimeError(msg)

        return self._run_in_subprocess()

    @utils.time_it
    def run_with_cost(self) -> tuple[tuple[float, float, float, float], float]:
        """
        Search all test data in serial.
        Returns:
            tuple[tuple[float, float, float, float], float]: (avg_recall, avg_ndcg, p99_latency, p95_latency), cost
        """
        log.info(f"{mp.current_process().name:14} start serial search")
        if self.test_data is None:
            msg = "empty test_data"
            raise RuntimeError(msg)

        return self._run_in_subprocess()
