import tempfile
from pathlib import Path

from pydantic import BaseModel

from ..api import DBCaseConfig, DBConfig, MetricType

DEFAULT_TURBOVEC_PATH = str(Path(tempfile.gettempdir()) / "vectordbbench_turbovec")


class TurboVecConfig(DBConfig):
    bit_width: int = 4
    # turbovec is an embedded, in-process index (no server). VectorDBBench runs
    # load/optimize/search in separate subprocesses, so this directory is where
    # the index gets persisted between phases instead of a real DB connection.
    path: str = DEFAULT_TURBOVEC_PATH
    # Determines the .tvim filename (<path>/<collection_name>.tvim). Give
    # different runs distinct names so they don't overwrite each other's
    # persisted index.
    collection_name: str = "turbovec_bench"

    def to_dict(self) -> dict:
        return {
            "db_label": self.db_label,
            "bit_width": self.bit_width,
            "path": self.path,
            "collection_name": self.collection_name,
        }


class TurboVecIndexConfig(BaseModel, DBCaseConfig):
    # turbovec scores a length-renormalized inner product. COSINE datasets are
    # handled via VectorDB.need_normalize_cosine() (VectorDBBench pre-normalizes
    # embeddings before insert/search), which makes IP scoring equivalent to
    # cosine similarity ranking.
    metric_type: MetricType | None = MetricType.COSINE

    def parse_metric(self) -> None:
        if self.metric_type not in (None, MetricType.COSINE, MetricType.IP):
            msg = f"Metric type {self.metric_type} is not supported by turbovec (only COSINE / IP)"
            raise ValueError(msg)

    def index_param(self) -> dict:
        self.parse_metric()
        return {}

    def search_param(self) -> dict:
        return {}
