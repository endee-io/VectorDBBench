import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
import concurrent.futures
import time

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosHttpResponseError

from ..api import DBCaseConfig, VectorDB

log = logging.getLogger(__name__)


class CosmosDB(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig,
        drop_old: bool = False,
        **kwargs,
    ):
        self.dim = dim
        self.db_config = db_config
        self.case_config = db_case_config
        # self.drop_old = drop_old

        # --- FIX: Force keep-old to resume from checkpoint ---
        # This completely ignores the CLI's --drop-old flag
        self.drop_old = False

        self.client = None
        self.db = None
        self.container = None

        log.info("Starting Cosmos DB Client")

    @contextmanager
    def init(self) -> Generator[None, None, None]:
        """Create and destroy connections to database."""
        try:
            endpoint = self.db_config.get("endpoint")
            key = self.db_config.get("key")
            
            self.client = CosmosClient(url=endpoint, credential=key)
            db_name = "VectorDBBenchDB"
            container_name = "VectorDBBenchContainer"

            # 1. Ensure DB exists
            self.db = self.client.create_database_if_not_exists(id=db_name)

            # 2. Handle drop_old
            if self.drop_old:
                try:
                    self.db.delete_container(container_name)
                except Exception:
                    pass

            # 3. Map Metric
            metric_map = {
                "L2": "euclidean",
                "IP": "dotproduct",
                "COSINE": "cosine"
            }
            metric_str = str(self.case_config.metric_type).upper().split('.')[-1]
            distance_function = metric_map.get(metric_str, "cosine")

            # 4. Define Vector Policies
            vector_embedding_policy = {
                "vectorEmbeddings": [
                    {
                        "path": "/vector",
                        "dataType": "float32",
                        "dimensions": self.dim,
                        "distanceFunction": distance_function
                    }
                ]
            }

            indexing_policy = {
                "indexingMode": "consistent",
                "includedPaths": [{"path": "/*"}],
                # Best practice: Azure requires vector paths to be excluded from normal indexing
                "excludedPaths": [{"path": "/_etag/?"}, {"path": "/vector/?"}],
                "vectorIndexes": [
                    # Changed from diskANN to quantizedFlat (Globally available)
                    {"path": "/vector", "type": "diskANN"}
                ]
            }

            # 5. Create Container
            self.container = self.db.create_container_if_not_exists(
                id=container_name,
                partition_key=PartitionKey(path="/id"),
                indexing_policy=indexing_policy,
                vector_embedding_policy=vector_embedding_policy
            )
            
        except Exception as e:
            # If Azure fails here, extract the raw string and raise a clean, picklable error
            err_msg = f"Cosmos DB Init Failed: {str(e)}"
            log.error(err_msg)
            raise RuntimeError(err_msg)
            
        # Yield outside the try-block so benchmarking errors don't trigger the init exception handler
        yield

    def optimize(self, data_size: int | None = None):
        # Cosmos DB handles index optimization automatically in the background.
        pass

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> tuple[int, Exception | None]:
        """Insert embeddings concurrently with 429-aware backoff."""
        total = len(metadata)
        
        # Tune this based on your RU/s. Start with 10, increase if no 429s.
        MAX_WORKERS = 15
        MAX_RETRIES_PER_DOC = 50
        
        def _upsert_one(vec, meta_id):
            doc = {"id": str(meta_id), "vector": vec, "meta_id": meta_id}
            retries = 0
            while True:
                try:
                    self.container.upsert_item(doc)
                    return
                except CosmosHttpResponseError as e:
                    if e.status_code == 429:
                        if retries >= MAX_RETRIES_PER_DOC:
                            raise Exception(f"429 retries exhausted for id={meta_id}")
                        wait_ms = int(e.headers.get('x-ms-retry-after-ms', 1000))
                        time.sleep((wait_ms / 1000.0) + 0.05)
                        retries += 1
                    else:
                        raise
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = [ex.submit(_upsert_one, v, m) for v, m in zip(embeddings, metadata)]
                for f in concurrent.futures.as_completed(futures):
                    f.result()  # raise any exception
            return total, None
        except Exception as e:
            err_msg = f"Cosmos DB Insertion Failed: {str(e)}"
            log.error(err_msg)
            return 0, Exception(err_msg)


    # def insert_embeddings(
    #         self,
    #         embeddings: list[list[float]],
    #         metadata: list[int],
    #         **kwargs: Any,
    #     ) -> tuple[int, Exception | None]:
    #         """Insert embeddings into the database serially with smart backoff."""
    #         try:
    #             total_count = len(metadata)
    #             completed_count = 0

    #             for vec, meta_id in zip(embeddings, metadata):
    #                 doc = {
    #                     "id": str(meta_id),
    #                     "vector": vec,
    #                     "meta_id": meta_id 
    #                 }
                    
    #                 # Custom Smart Retry Loop for 429s
    #                 max_retries = 50 # Generous retry allowance
    #                 retries = 0
                    
    #                 while True:
    #                     try:
    #                         self.container.upsert_item(doc)
    #                         break # Success, break out of the while loop
                            
    #                     except CosmosHttpResponseError as e:
    #                         if e.status_code == 429:
    #                             if retries >= max_retries:
    #                                 raise Exception(f"Failed after {max_retries} retries due to strict Rate Limiting.")
                                
    #                             # Extract Azure's exact requested wait time (default to 1 second if missing)
    #                             wait_ms = int(e.headers.get('x-ms-retry-after-ms', 1000))
                                
    #                             # Sleep for the requested time + a tiny buffer
    #                             time.sleep((wait_ms / 1000.0) + 0.1)
    #                             retries += 1
    #                         else:
    #                             # If it's a different HTTP error (like 400 Bad Request), crash immediately
    #                             raise e

    #                 completed_count += 1
                    
    #                 if completed_count % 1000 == 0 or completed_count == total_count:
    #                     log.info(f"Progress: Upserted {completed_count} / {total_count} vectors...")
                    
    #             return total_count, None
                
    #         except Exception as e:
    #             err_msg = f"Cosmos DB Insertion Failed: {str(e)}"
    #             log.error(err_msg)
    #             return 0, Exception(err_msg)


    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> list[int]:
        """Search the database."""
        query_text = f"""
            SELECT TOP {k} c.meta_id
            FROM c 
            ORDER BY VectorDistance(c.vector, @embedding)
        """
        parameters = [{"name": "@embedding", "value": query}]
        
        results = list(self.container.query_items(
            query=query_text,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        return [res["meta_id"] for res in results]