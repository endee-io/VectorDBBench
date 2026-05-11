from pydantic import BaseModel

from ..api import DBCaseConfig, DBConfig, MetricType


class VertexAIMatchingEngineConfig(DBConfig):
    """Connection / infrastructure config for Vertex AI Vector Search (Matching Engine).

    Authentication via Application Default Credentials:
        gcloud auth application-default login
    or set GOOGLE_APPLICATION_CREDENTIALS to a service-account key file.

    Args:
        project:                GCP project ID.
        location:               GCP region (default: us-central1).
        index_display_name:     Display name for the MatchingEngineIndex resource.
        endpoint_display_name:  Display name for the IndexEndpoint resource.
        deployed_index_id:      ID used when deploying the index to the endpoint.
                                Must be unique within the endpoint and match [a-z0-9_].
        machine_type:           VM type for the endpoint replicas (None = API default).
        min_replica_count:      Minimum number of replicas (default 1).
        max_replica_count:      Maximum number of replicas (default 1).
    """

    project: str
    location: str = "us-central1"
    index_display_name: str = "vectordbbench-index"
    endpoint_display_name: str = "vectordbbench-endpoint"
    deployed_index_id: str = "vectordbbench_deployed"
    machine_type: str | None = None
    min_replica_count: int = 1
    max_replica_count: int = 1
    shard_size: str = "SHARD_SIZE_SMALL"
    gcs_bucket: str | None = None
    reuse_gcs_folder: str | None = None

    def to_dict(self) -> dict:
        return {
            "project": self.project,
            "location": self.location,
            "index_display_name": self.index_display_name,
            "endpoint_display_name": self.endpoint_display_name,
            "deployed_index_id": self.deployed_index_id,
            "machine_type": self.machine_type,
            "min_replica_count": self.min_replica_count,
            "max_replica_count": self.max_replica_count,
            "shard_size": self.shard_size,
            "gcs_bucket": self.gcs_bucket,
            "reuse_gcs_folder": self.reuse_gcs_folder,
        }


class VertexAIMatchingEngineIndexConfig(BaseModel, DBCaseConfig):
    """Index tuning parameters for the tree-AH ScaNN index.

    Args:
        metric_type:                    Distance metric (COSINE, L2, IP).
        approximate_neighbors_count:    Target number of neighbors per leaf to
                                        consider at query time (higher = better
                                        recall, slower queries).
        leaf_node_embedding_count:      Number of embeddings per leaf node
                                        (controls tree branching factor).
        leaf_nodes_to_search_percent:   Percentage of leaf nodes to search per
                                        query (1–100; higher = better recall).
        fraction_leaf_nodes_to_search_override:
                                        Per-query override (0.0–1.0); None means
                                        use the index default.
    """

    metric_type: MetricType | None = None
    approximate_neighbors_count: int = 128
    leaf_node_embedding_count: int = 1000
    leaf_nodes_to_search_percent: int = 5
    fraction_leaf_nodes_to_search_override: float | None = None

    def index_param(self) -> dict:
        return {
            "metric_type": self.metric_type,
            "approximate_neighbors_count": self.approximate_neighbors_count,
            "leaf_node_embedding_count": self.leaf_node_embedding_count,
            "leaf_nodes_to_search_percent": self.leaf_nodes_to_search_percent,
        }

    def search_param(self) -> dict:
        return {
            "fraction_leaf_nodes_to_search_override": self.fraction_leaf_nodes_to_search_override,
        }
