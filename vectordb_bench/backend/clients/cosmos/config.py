from pydantic import BaseModel

from ..api import DBCaseConfig, DBConfig, MetricType


class CosmosDBConfig(DBConfig):
    endpoint: str
    key: str

    def to_dict(self) -> dict:
        return {
            "db_label": self.db_label,
            "endpoint": self.endpoint,
            "key": self.key
        }


class CosmosDBIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None

    def index_param(self) -> dict:
        # Cosmos DB DiskANN doesn't require explicit client-side hyperparams like m/ef
        return {}

    def search_param(self) -> dict:
        return {}