import logging
import time
from contextlib import contextmanager
from typing import Any

import numpy as np
import redis
from redis.commands.search.field import NumericField, TagField, VectorField

try:  # redis-py >= 6 renamed the module
    from redis.commands.search.index_definition import IndexDefinition, IndexType
except ImportError:  # older redis-py
    from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from vectordb_bench.backend.filter import Filter, FilterOp

from ..api import DBCaseConfig, VectorDB

log = logging.getLogger(__name__)
INDEX_NAME = "index"  # Vector Index Name


def encode_vector(vec: list[float], vector_type: str) -> bytes:
    """Serialise a vector into the byte layout Redis expects for the given TYPE."""
    arr = np.asarray(vec, dtype=np.float32)
    if vector_type == "FLOAT16":
        return arr.astype(np.float16).tobytes()
    if vector_type == "BFLOAT16":
        # bfloat16 = top 16 bits of float32; round-to-nearest-even before truncating
        bits = arr.view(np.uint32)
        bits = bits + 0x7FFF + ((bits >> 16) & 1)
        return (bits >> 16).astype(np.uint16).tobytes()
    return arr.tobytes()


def _escape_tag(value: str) -> str:
    """Escape a TAG query value: every character that is not a letter, digit or underscore gets a backslash."""
    return "".join(c if c.isalnum() or c == "_" else f"\\{c}" for c in value)


class Redis(VectorDB):
    supported_filter_types: list[FilterOp] = [
        FilterOp.NonFilter,
        FilterOp.NumGE,
        FilterOp.StrEqual,
    ]

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig,
        drop_old: bool = False,
        collection_name: str | None = None,  # task_runner passes the --collection-name value here
        with_scalar_labels: bool = False,
        **kwargs,
    ):
        self.db_config = db_config
        # label-filter cases: index a TAG field "label" (only then, so the unfiltered index stays unchanged)
        self.with_scalar_labels = with_scalar_labels
        self.filter_prefix = "*"
        self.case_config = db_case_config
        # --collection-name: index name. The default "index" keeps the legacy layout (bare integer keys, no prefix).
        # Any other name gets its own key prefix so several indexes can live side by side without indexing each other's hashes.
        self.index_name = collection_name or self.db_config.get("collection_name") or INDEX_NAME
        self.collection_name = self.index_name
        self._key_prefix = "" if self.index_name == INDEX_NAME else f"{self.index_name}:"

        # Create a redis connection, if db has password configured, add it to the connection here and in init():
        password = self.db_config["password"]
        conn = redis.Redis(
            host=self.db_config["host"],
            port=self.db_config["port"],
            password=password,
            db=0,
        )

        if drop_old:
            try:
                conn.ft(self.index_name).info()
                # a named index owns its key prefix, so dropping it also deletes its documents (DD);
                # the default "index" keeps the legacy behaviour (documents are overwritten by the next load)
                conn.ft(self.index_name).dropindex(delete_documents=bool(self._key_prefix))
            except redis.exceptions.ResponseError:
                drop_old = False
                log.info(f"Redis client drop_old collection: {self.collection_name}")

        self.make_index(dim, conn)
        conn.close()
        conn = None

    def make_index(self, vector_dimensions: int, conn: redis.Redis):
        try:
            # check to see if index exists
            conn.ft(self.index_name).info()
        except Exception:
            label_fields = (TagField("label"),) if self.with_scalar_labels else ()
            schema = (
                TagField("id"),
                NumericField("metadata"),
                *label_fields,
                VectorField(
                    "vector",  # Vector Field Name
                    "HNSW",  # Vector Index Type: FLAT or HNSW
                    {
                        "TYPE": self.case_config.vector_type,  # FLOAT32 / FLOAT16 / BFLOAT16
                        "DIM": vector_dimensions,  # Number of Vector Dimensions
                        "DISTANCE_METRIC": self.case_config.parse_metric() or "COSINE",  # COSINE / L2 / IP
                        "M": self.case_config.index_param()["params"]["M"],
                        "EF_CONSTRUCTION": self.case_config.index_param()["params"]["efConstruction"],
                    },
                ),
            )

            definition = IndexDefinition(index_type=IndexType.HASH, prefix=[self._key_prefix] if self._key_prefix else [])

            rs = conn.ft(self.index_name)
            rs.create_index(schema, definition=definition)

    @contextmanager
    def init(self) -> None:
        """create and destory connections to database.

        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
        """
        self.conn = redis.Redis(
            host=self.db_config["host"],
            port=self.db_config["port"],
            password=self.db_config["password"],
            db=0,
        )
        yield
        self.conn.close()
        self.conn = None

    def ready_to_search(self) -> bool:
        """Check if the database is ready to search."""

    def optimize(self, data_size: int | None = None):
        """Block until Redis has finished indexing.

        HSET returns before the document is added to the HNSW index (indexing is
        asynchronous), so without this the search phase can start against a
        partially built index and report inflated QPS and deflated recall.
        """
        assert self.conn is not None
        while True:
            info = self.conn.ft(self.index_name).info()
            indexing = int(info.get("indexing", 0))
            pct = float(info.get("percent_indexed", 1))
            if indexing == 0 and pct >= 1.0:
                log.info(f"Redis index fully built: num_docs={info.get('num_docs')}")
                return
            log.debug(f"Redis indexing... {pct:.2%}")
            time.sleep(2)

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        **kwargs: Any,
    ) -> tuple[int, Exception]:
        """Insert embeddings into the database.
        Should call self.init() first.
        """

        batch_size = 1000  # Adjust this as needed, but don't make too big
        try:
            with self.conn.pipeline(transaction=False) as pipe:
                for i, embedding in enumerate(embeddings):
                    mapping = {
                        "id": str(metadata[i]),
                        "metadata": metadata[i],
                        "vector": encode_vector(embedding, self.case_config.vector_type),
                    }
                    if self.with_scalar_labels and labels_data is not None:
                        mapping["label"] = labels_data[i]
                    pipe.hset(f"{self._key_prefix}{metadata[i]}", mapping=mapping)
                    # Execute the pipe so we don't keep too much in memory at once
                    if i % batch_size == 0:
                        _ = pipe.execute()

                _ = pipe.execute()
                result_len = i + 1
        except Exception as e:
            return 0, e

        return result_len, None

    def prepare_filter(self, filters: Filter):
        """VectorDBBench filters: NumGE = id >= int_value (inclusive), StrEqual = label == label_value.
        Used as the pre-filter of the KNN query (Redis hybrid query)."""
        if filters.type == FilterOp.NonFilter:
            self.filter_prefix = "*"
        elif filters.type == FilterOp.NumGE:
            self.filter_prefix = f"(@metadata:[{int(filters.int_value)} +inf])"
        elif filters.type == FilterOp.StrEqual:
            self.filter_prefix = f"(@label:{{{_escape_tag(str(filters.label_value))}}})"
        else:
            msg = f"Not support Filter for Redis - {filters}"
            raise ValueError(msg)

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> list[int]:
        assert self.conn is not None

        query_vector = encode_vector(query, self.case_config.vector_type)
        ef_runtime = self.case_config.search_param()["params"]["ef"]
        policy = ""
        hp = getattr(self.case_config, "hybrid_policy", None)
        if hp and self.filter_prefix != "*":  # the hybrid policy only applies to filtered queries
            policy = f" HYBRID_POLICY {hp}"
            if hp == "BATCHES" and getattr(self.case_config, "batch_size", None):
                policy += f" BATCH_SIZE {int(self.case_config.batch_size)}"
        # ADHOC_BF = exact brute force over the filtered documents: Redis rejects EF_RUNTIME there
        # (SEARCH_ADHOC_EF_RUNTIME_IRRELEVANT), so only the HNSW paths get EF_RUNTIME
        ef_part = "" if hp == "ADHOC_BF" and policy else f" EF_RUNTIME {ef_runtime}"
        query_obj = (
            Query(f"{self.filter_prefix}=>[KNN {k} @vector $vec{ef_part}{policy} as score]")
            .sort_by("score")
            .return_fields("id", "score")
            .paging(0, k)
            .dialect(2)
        )
        query_params = {"vec": query_vector}
        res = self.conn.ft(self.index_name).search(query_obj, query_params)
        # doc in res of format {'id': '9831', 'payload': None, 'score': '1.19209289551e-07'}
        # doc.id is the Redis key: "<id>" for the default index, "<collection>:<id>" for a named one (--collection-name)
        return [int(str(doc.id)[len(self._key_prefix):]) for doc in res.docs]
