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


class Redis(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig,
        drop_old: bool = False,
        collection_name: str | None = None,  # task_runner passes the --collection-name value here
        **kwargs,
    ):
        self.db_config = db_config
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
            schema = (
                TagField("id"),
                NumericField("metadata"),
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
        **kwargs: Any,
    ) -> tuple[int, Exception]:
        """Insert embeddings into the database.
        Should call self.init() first.
        """

        batch_size = 1000  # Adjust this as needed, but don't make too big
        try:
            with self.conn.pipeline(transaction=False) as pipe:
                for i, embedding in enumerate(embeddings):
                    pipe.hset(
                        f"{self._key_prefix}{metadata[i]}",
                        mapping={
                            "id": str(metadata[i]),
                            "metadata": metadata[i],
                            "vector": encode_vector(embedding, self.case_config.vector_type),
                        },
                    )
                    # Execute the pipe so we don't keep too much in memory at once
                    if i % batch_size == 0:
                        _ = pipe.execute()

                _ = pipe.execute()
                result_len = i + 1
        except Exception as e:
            return 0, e

        return result_len, None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> list[int]:
        assert self.conn is not None

        query_vector = encode_vector(query, self.case_config.vector_type)
        ef_runtime = self.case_config.search_param()["params"]["ef"]
        query_obj = (
            Query(f"*=>[KNN {k} @vector $vec EF_RUNTIME {ef_runtime} as score]")
            .sort_by("score")
            .return_fields("id", "score")
            .paging(0, k)
            .dialect(2)
        )
        query_params = {"vec": query_vector}

        if filters:
            # benchmark test filters of format: {'metadata': '>=10000', 'id': 10000}
            # gets exact match for id, and range for metadata if they exist in filters
            id_value = filters.get("id")
            metadata_value = filters.get("metadata")
            if id_value and metadata_value:
                query_obj = (
                    Query(
                        f"(@metadata:[{metadata_value} +inf] @id:{ {id_value} })=>[KNN {k} ",
                        f"@vector $vec EF_RUNTIME {ef_runtime} as score]",
                    )
                    .sort_by("score")
                    .return_fields("id", "score")
                    .paging(0, k)
                    .dialect(2)
                )
            elif id_value:
                # gets exact match for id
                query_obj = (
                    Query(f"@id:{ {id_value} }=>[KNN {k} @vector $vec EF_RUNTIME {ef_runtime} as score]")
                    .sort_by("score")
                    .return_fields("id", "score")
                    .paging(0, k)
                    .dialect(2)
                )
            else:  # metadata only case, greater than or equal to metadata value
                query_obj = (
                    Query(f"@metadata:[{metadata_value} +inf]=>[KNN {k} @vector $vec EF_RUNTIME {ef_runtime} as score]")
                    .sort_by("score")
                    .return_fields("id", "score")
                    .paging(0, k)
                    .dialect(2)
                )
        res = self.conn.ft(self.index_name).search(query_obj, query_params)
        # doc in res of format {'id': '9831', 'payload': None, 'score': '1.19209289551e-07'}
        # doc.id is the Redis key: "<id>" for the default index, "<collection>:<id>" for a named one (--collection-name)
        return [int(str(doc.id)[len(self._key_prefix):]) for doc in res.docs]
