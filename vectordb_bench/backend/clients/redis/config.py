from typing import Literal

from pydantic import BaseModel, SecretStr

from ..api import DBCaseConfig, DBConfig, IndexType, MetricType


class RedisConfig(DBConfig):
    password: SecretStr | None = None
    host: SecretStr
    port: int | None = None
    collection_name: str | None = None  # index name; None -> "index"

    def to_dict(self) -> dict:
        return {
            "collection_name": self.collection_name or None,
            "host": self.host.get_secret_value(),
            "port": self.port,
            "password": self.password.get_secret_value() if self.password is not None else None,
        }


class RedisIndexConfig(BaseModel):
    """Base config for milvus"""

    metric_type: MetricType | None = None
    # Redis Query Engine vector field TYPE: FLOAT32 (default), FLOAT16, BFLOAT16 (FLOAT64 is wider, not useful)
    vector_type: Literal["FLOAT32", "FLOAT16", "BFLOAT16"] = "FLOAT32"

    def parse_metric(self) -> str:
        if not self.metric_type:
            return ""
        return self.metric_type.value


class RedisHNSWConfig(RedisIndexConfig, DBCaseConfig):
    M: int
    efConstruction: int
    ef: int | None = None
    index: IndexType = IndexType.HNSW
    # filtered KNN execution mode (Redis docs, runtime query parameters): None = Redis chooses automatically,
    # "ADHOC_BF" = exact brute force over the documents that pass the filter, "BATCHES" = paged HNSW batches
    hybrid_policy: str | None = None
    batch_size: int | None = None

    def index_param(self) -> dict:
        return {
            "metric_type": self.parse_metric(),
            "index_type": self.index.value,
            "params": {"M": self.M, "efConstruction": self.efConstruction},
        }

    def search_param(self) -> dict:
        return {
            "metric_type": self.parse_metric(),
            "params": {"ef": self.ef},
        }
