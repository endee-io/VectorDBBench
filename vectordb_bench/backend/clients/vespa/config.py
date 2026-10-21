from typing import Literal, TypeAlias

from pydantic import BaseModel, SecretStr

from ..api import DBCaseConfig, DBConfig, MetricType

VespaMetric: TypeAlias = Literal["euclidean", "angular", "dotproduct", "prenormalized-angular", "hamming", "geodegrees"]

VespaQuantizationType: TypeAlias = Literal["none", "binary", "bfloat16", "int8"]


class VespaConfig(DBConfig):
    url: SecretStr = "http://127.0.0.1"
    port: int = 8080

    def to_dict(self):
        return {
            "url": self.url.get_secret_value(),
            "port": self.port,
        }


class VespaHNSWConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType = MetricType.COSINE
    quantization_type: VespaQuantizationType = "none"
    M: int = 16
    efConstruction: int = 200
    ef: int = 100
    # Largest absolute component across the corpus, used to derive the int8 scale.
    # Must be measured per dataset (`python -m vectordb_bench.tools.int8_scale <parquet>`):
    # the benchmark vectors are not unit-normalized, so there is no safe default.
    int8_max_abs: float | None = None

    @property
    def int8_scale(self) -> float:
        """Factor mapping the corpus maximum onto the int8 ceiling of 127.

        A single global scalar is required: cosine is invariant to a per-vector
        scalar, so this leaves rankings untouched, whereas per-dimension scaling
        would stretch the axes independently and distort the metric.
        """
        if not self.int8_max_abs or self.int8_max_abs <= 0:
            msg = (
                "quantization_type='int8' needs int8_max_abs (the dataset's largest absolute "
                "component). Measure it with `python -m vectordb_bench.tools.int8_scale "
                "<train.parquet>` and pass --int8-max-abs. Note these vectors are NOT "
                "unit-normalized, so a fixed 127 scale overflows int8 and corrupts every vector."
            )
            raise ValueError(msg)
        return 127.0 / self.int8_max_abs

    def index_param(self) -> dict:
        return {
            "distance_metric": self.parse_metric(self.metric_type),
            "max_links_per_node": self.M,
            "neighbors_to_explore_at_insert": self.efConstruction,
        }

    def search_param(self) -> dict:
        return {}

    def parse_metric(self, metric_type: MetricType) -> VespaMetric:
        match metric_type:
            case MetricType.COSINE:
                return "angular"
            case MetricType.L2:
                return "euclidean"
            case MetricType.DP | MetricType.IP:
                return "dotproduct"
            case MetricType.HAMMING:
                return "hamming"
            case _:
                raise NotImplementedError
