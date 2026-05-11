"""VectorDBBench client for Vertex AI Vector Search (classic Matching Engine API).

Uses google-cloud-aiplatform:
    pip install google-cloud-aiplatform

Architecture:
    MatchingEngineIndex  (tree-AH ScaNN, BATCH_UPDATE mode)
        └── deployed to MatchingEngineIndexEndpoint  (public endpoint)

Insert flow (BATCH_UPDATE):
    1. insert_embeddings() writes vectors to a local JSONL buffer.
       When the buffer reaches _GCS_CHUNK_BYTES, it is uploaded to GCS
       as a numbered part file and the local buffer is cleared.
    2. optimize() flushes any remaining local data to GCS, triggers
       index.update_embeddings() on the GCS folder, deploys the index
       to the endpoint, then probes until ready.

Authentication: Application Default Credentials
    gcloud auth application-default login
or set GOOGLE_APPLICATION_CREDENTIALS to a service-account key file.
"""

import json
import logging
import os
import time
from contextlib import contextmanager

from vectordb_bench.backend.filter import FilterOp

from ..api import DBCaseConfig, MetricType, VectorDB

log = logging.getLogger(__name__)

# Seconds to wait between warm-up probes in optimize().
_PROBE_INTERVAL_S = 15

# Maximum seconds to wait for the endpoint to become ready in optimize().
_WARMUP_TIMEOUT_S = 1800

# Flush local JSONL buffer to GCS after it exceeds this size.
_GCS_CHUNK_BYTES = 400 * 1024 * 1024  # 400 MB


class VertexAIMatchingEngine(VectorDB):
    """Vertex AI Vector Search client (MatchingEngineIndex + IndexEndpoint)."""

    name = "VertexAIMatchingEngine"
    supported_filter_types = [FilterOp.NonFilter]

    # ------------------------------------------------------------------ #
    # Construction / one-time infrastructure setup
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig,
        collection_name: str = "vectordbbench",
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.dim = dim
        self.project = db_config["project"]
        self.location = db_config.get("location", "us-central1")
        self.index_display_name = db_config.get("index_display_name", "vectordbbench-index")
        self.endpoint_display_name = db_config.get("endpoint_display_name", "vectordbbench-endpoint")
        self.deployed_index_id = db_config.get("deployed_index_id", "vectordbbench_deployed")
        self.machine_type = db_config.get("machine_type")
        self.min_replica_count = db_config.get("min_replica_count", 1)
        self.max_replica_count = db_config.get("max_replica_count", 1)
        self.shard_size = db_config.get("shard_size", "SHARD_SIZE_SMALL")
        self.gcs_bucket = db_config.get("gcs_bucket") or ""
        self.reuse_gcs_folder = db_config.get("reuse_gcs_folder") or None
        self.case_config = db_case_config

        # Local JSONL buffer — flushed to GCS in chunks so disk usage stays bounded.
        safe_name = self.index_display_name.replace("-", "_").replace(" ", "_")
        self._batch_jsonl_path = f"/tmp/vdb_batch_{safe_name}.jsonl"

        # Tracks how many part files have been uploaded to GCS this run.
        self._gcs_part_count = 0

        # Resource names (plain strings) survive process spawning; the actual
        # SDK objects are re-created inside init() for each worker.
        self._index_resource_name: str | None = None
        self._endpoint_resource_name: str | None = None

        # Per-init() handles (None outside init context).
        self._index = None
        self._endpoint = None

        from google.cloud import aiplatform
        aiplatform.init(project=self.project, location=self.location)

        if drop_old:
            self._teardown()
            with open(self._batch_jsonl_path, "w"):
                pass
            if self.gcs_bucket and not self.reuse_gcs_folder:
                self._clear_gcs_folder()

        index = self._ensure_index()
        endpoint = self._ensure_endpoint()

        self._index_resource_name = index.resource_name
        self._endpoint_resource_name = endpoint.resource_name

    # ------------------------------------------------------------------ #
    # Distance / metric helpers
    # ------------------------------------------------------------------ #

    def _distance_measure(self) -> str:
        metric = self.case_config.index_param().get("metric_type")
        if metric == MetricType.L2:
            return "SQUARED_L2_DISTANCE"
        return "DOT_PRODUCT_DISTANCE"

    def need_normalize_cosine(self) -> bool:
        return self.case_config.index_param().get("metric_type") == MetricType.COSINE

    # ------------------------------------------------------------------ #
    # Infrastructure helpers (main-process only)
    # ------------------------------------------------------------------ #

    @property
    def _gcs_folder(self) -> str:
        if self.reuse_gcs_folder:
            return self.reuse_gcs_folder.rstrip("/") + "/"
        return f"vectordbbench/{self.index_display_name}/"

    def _list_indexes(self):
        from google.cloud import aiplatform
        return aiplatform.MatchingEngineIndex.list(
            filter=f'display_name="{self.index_display_name}"'
        )

    def _list_endpoints(self):
        from google.cloud import aiplatform
        return aiplatform.MatchingEngineIndexEndpoint.list(
            filter=f'display_name="{self.endpoint_display_name}"'
        )

    def _teardown(self) -> None:
        log.info("Tearing down index and endpoint (display names: '%s', '%s')...",
                 self.index_display_name, self.endpoint_display_name)

        target_indexes = self._list_indexes()
        target_index_names = {idx.resource_name for idx in target_indexes}

        from google.cloud import aiplatform
        for ep in aiplatform.MatchingEngineIndexEndpoint.list():
            to_undeploy = [d.id for d in ep.deployed_indexes if d.index in target_index_names]
            for deployed_id in to_undeploy:
                log.info("Undeploying '%s' from endpoint '%s'...", deployed_id, ep.display_name)
                ep.undeploy_index(deployed_index_id=deployed_id)

            if ep.display_name == self.endpoint_display_name:
                log.info("Deleting endpoint '%s'...", ep.display_name)
                ep.delete(force=True)
                log.info("Endpoint deleted.")

        for idx in target_indexes:
            log.info("Deleting index '%s'...", idx.display_name)
            idx.delete()
            log.info("Index deleted.")

        log.info("Teardown complete.")

    def _clear_gcs_folder(self) -> None:
        """Delete all objects in the GCS folder for a clean run."""
        from google.cloud import storage
        client = storage.Client(project=self.project)
        bucket = client.bucket(self.gcs_bucket)
        blobs = list(bucket.list_blobs(prefix=self._gcs_folder))
        if blobs:
            log.info(
                "Clearing %d existing GCS objects in gs://%s/%s ...",
                len(blobs), self.gcs_bucket, self._gcs_folder,
            )
            bucket.delete_blobs(blobs)
            log.info("GCS folder cleared.")

    def _count_gcs_parts(self) -> int:
        """Count existing part files in GCS folder (used to avoid overwriting on subprocess restart)."""
        from google.cloud import storage
        client = storage.Client(project=self.project)
        bucket = client.bucket(self.gcs_bucket)
        return sum(
            1 for b in bucket.list_blobs(prefix=self._gcs_folder)
            if b.name.endswith(".json")
        )

    def _flush_chunk_to_gcs(self) -> None:
        """Upload current local JSONL buffer to GCS as a numbered part file, then clear it."""
        from google.cloud import storage
        # Use GCS state as source of truth for part number so insert and optimize
        # subprocesses (each starting with _gcs_part_count=0) never overwrite each other.
        part_num = max(self._gcs_part_count, self._count_gcs_parts())
        blob_name = f"{self._gcs_folder}part_{part_num:04d}.json"
        log.info("Flushing chunk to gs://%s/%s ...", self.gcs_bucket, blob_name)
        client = storage.Client(project=self.project)
        bucket = client.bucket(self.gcs_bucket)
        bucket.blob(blob_name).upload_from_filename(self._batch_jsonl_path)
        self._gcs_part_count = part_num + 1
        with open(self._batch_jsonl_path, "w"):
            pass
        log.info("Chunk uploaded and local buffer cleared.")

    def _ensure_index(self):
        from google.cloud import aiplatform
        existing = self._list_indexes()
        if existing:
            log.info("Using existing index: %s", existing[0].resource_name)
            return existing[0]

        params = self.case_config.index_param()
        log.info(
            "Creating tree-AH index '%s' (dim=%d, distance=%s, BATCH_UPDATE) — takes ~5 min...",
            self.index_display_name, self.dim, self._distance_measure(),
        )
        index = aiplatform.MatchingEngineIndex.create_tree_ah_index(
            display_name=self.index_display_name,
            dimensions=self.dim,
            approximate_neighbors_count=params.get("approximate_neighbors_count", 128),
            distance_measure_type=self._distance_measure(),
            leaf_node_embedding_count=params.get("leaf_node_embedding_count", 1000),
            leaf_nodes_to_search_percent=params.get("leaf_nodes_to_search_percent", 5),
            index_update_method="BATCH_UPDATE",
            shard_size=self.shard_size,
        )
        log.info("Index created: %s", index.resource_name)
        return index

    def _ensure_endpoint(self):
        from google.cloud import aiplatform
        existing = self._list_endpoints()
        if existing:
            log.info("Using existing endpoint: %s", existing[0].resource_name)
            return existing[0]

        log.info("Creating public index endpoint '%s'...", self.endpoint_display_name)
        try:
            endpoint = aiplatform.MatchingEngineIndexEndpoint.create(
                display_name=self.endpoint_display_name,
                public_endpoint_enabled=True,
            )
            log.info("Endpoint created: %s", endpoint.resource_name)
            return endpoint
        except Exception as e:
            log.info("Endpoint create raised %s — polling for it...", e)
            for _ in range(12):
                time.sleep(10)
                existing = self._list_endpoints()
                if existing:
                    log.info("Found endpoint after poll: %s", existing[0].resource_name)
                    return existing[0]
            raise

    def _deploy_sync(self, index, endpoint) -> None:
        """Deploy the index synchronously, retrying if the ID is still being cleaned up."""
        deploy_kwargs = dict(
            index=index,
            deployed_index_id=self.deployed_index_id,
            display_name=self.deployed_index_id,
            min_replica_count=self.min_replica_count,
            max_replica_count=self.max_replica_count,
        )
        if self.machine_type:
            deploy_kwargs["machine_type"] = self.machine_type

        for attempt in range(10):
            try:
                log.info(
                    "Deploying index as '%s' — first deployment can take ~30 min...",
                    self.deployed_index_id,
                )
                endpoint.deploy_index(**deploy_kwargs)
                log.info("Index deployed successfully as '%s'.", self.deployed_index_id)
                return
            except Exception as e:
                if "being undeployed" in str(e) or "failed state" in str(e):
                    log.info(
                        "Deployed index ID '%s' still cleaning up (attempt %d/10) — retrying in 30s...",
                        self.deployed_index_id, attempt + 1,
                    )
                    time.sleep(30)
                else:
                    raise

        raise RuntimeError(
            f"Deployed index ID '{self.deployed_index_id}' still cleaning up after 5 min. "
            "Pass a different --deployed-index-id or wait and retry."
        )

    # ------------------------------------------------------------------ #
    # VectorDB interface
    # ------------------------------------------------------------------ #

    @contextmanager
    def init(self):
        from google.cloud import aiplatform
        aiplatform.init(project=self.project, location=self.location)
        self._index = aiplatform.MatchingEngineIndex(
            index_name=self._index_resource_name
        )
        self._endpoint = aiplatform.MatchingEngineIndexEndpoint(
            index_endpoint_name=self._endpoint_resource_name
        )
        try:
            yield
        finally:
            self._index = None
            self._endpoint = None

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        **kwargs,
    ) -> tuple[int, Exception | None]:
        assert len(embeddings) == len(metadata)

        # Skip local writes — data already exists in GCS.
        if self.reuse_gcs_folder:
            return len(embeddings), None

        insert_count = 0
        try:
            with open(self._batch_jsonl_path, "a") as f:
                for i, embedding in enumerate(embeddings):
                    json.dump({"id": str(metadata[i]), "embedding": embedding}, f)
                    f.write("\n")
                    insert_count += 1
        except Exception as e:
            log.warning("Insert failed after %d vectors: %s", insert_count, e)
            return insert_count, e

        # Flush to GCS when local buffer grows too large to avoid filling disk.
        # Separated from the write try/except so a GCS failure propagates cleanly
        # without leaving partial state that could cause duplicate writes on retry.
        if (
            self.gcs_bucket
            and os.path.getsize(self._batch_jsonl_path) >= _GCS_CHUNK_BYTES
        ):
            self._flush_chunk_to_gcs()

        return len(embeddings), None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        **kwargs,
    ) -> list[int]:
        assert self._endpoint is not None, "Call init() before search_embedding()"

        search_params = self.case_config.search_param()
        call_kwargs = {}
        frac = search_params.get("fraction_leaf_nodes_to_search_override")
        if frac is not None:
            call_kwargs["fraction_leaf_nodes_to_search_override"] = frac

        while True:
            try:
                response = self._endpoint.find_neighbors(
                    deployed_index_id=self.deployed_index_id,
                    queries=[query],
                    num_neighbors=k,
                    **call_kwargs,
                )
                break
            except Exception as e:
                if "503" in str(e) or "connection attempt timed out" in str(e):
                    time.sleep(2)
                else:
                    raise

        if not response:
            return []
        return [int(neighbor.id) for neighbor in response[0]]

    def optimize(self, data_size: int | None = None) -> None:
        """Flush remaining data to GCS, trigger batch update, deploy, then probe until ready."""
        assert self._index is not None, "Call init() before optimize()"
        assert self._endpoint is not None, "Call init() before optimize()"

        if not self.gcs_bucket:
            raise RuntimeError("gcs_bucket must be set for BATCH_UPDATE mode.")

        # 1. Flush any remaining local buffer to GCS.
        if os.path.exists(self._batch_jsonl_path) and os.path.getsize(self._batch_jsonl_path) > 0:
            self._flush_chunk_to_gcs()

        # 2. Trigger batch index update from the GCS folder.
        gcs_uri = f"gs://{self.gcs_bucket}/{self._gcs_folder}"
        log.info("Triggering batch update from %s — may take several minutes...", gcs_uri)
        self._index.update_embeddings(
            contents_delta_uri=gcs_uri,
            is_complete_overwrite=True,
        )
        log.info("Batch update complete.")

        # 3. Deploy index to endpoint if not already deployed.
        deployed_ids = {d.id for d in self._endpoint.deployed_indexes}
        if self.deployed_index_id not in deployed_ids:
            self._deploy_sync(self._index, self._endpoint)
        else:
            log.info("Index already deployed as '%s'.", self.deployed_index_id)

        # 4. Probe until endpoint is serving queries.
        dummy = [0.0] * self.dim
        deadline = time.monotonic() + _WARMUP_TIMEOUT_S
        attempt = 0

        while time.monotonic() < deadline:
            try:
                self._endpoint.find_neighbors(
                    deployed_index_id=self.deployed_index_id,
                    queries=[dummy],
                    num_neighbors=1,
                )
                log.info("Endpoint ready after %d probe attempt(s).", attempt + 1)
                return
            except Exception as e:
                attempt += 1
                log.info(
                    "Endpoint not ready yet (attempt %d): %s — retrying in %ds...",
                    attempt, e, _PROBE_INTERVAL_S,
                )
                time.sleep(_PROBE_INTERVAL_S)

        log.warning(
            "Endpoint warm-up timed out after %ds (%d attempts); proceeding anyway.",
            _WARMUP_TIMEOUT_S, attempt,
        )
