import logging

from pydantic import BaseModel

from vectordb_bench.backend.clients.api import DBCaseConfig, DBConfig, MetricType

log = logging.getLogger(__name__)

# The engine's space types, keyed by the dataset metric the framework reports.
_SPACE_TYPE_FOR_METRIC = {
    MetricType.COSINE: "cosine",
    MetricType.L2: "l2",
    MetricType.IP: "ip",
}


class EndeeLibConfig(DBConfig):
    """Connection config for the embedded Endee engine (nddlib).

    There is no server and no auth: the engine runs in-process against a local
    ``data_dir``. Only one ``data_dir`` may be open per process, which the
    benchmark honours by opening it inside ``init()``.
    """

    data_dir: str = "./endee_data"
    db: str = "default"
    collection_name: str = "endee_bench"

    # engine open options
    skip_sanity: bool = False
    save_on_shutdown: bool = True
    vector_cache_max_bytes: int = 0
    parallel_insert_threads: int = 0

    def to_dict(self) -> dict:
        return {
            "data_dir": self.data_dir,
            "db": self.db,
            "collection_name": self.collection_name,
            "skip_sanity": self.skip_sanity,
            "save_on_shutdown": self.save_on_shutdown,
            "vector_cache_max_bytes": self.vector_cache_max_bytes,
            "parallel_insert_threads": self.parallel_insert_threads,
        }


class EndeeLibIndexConfig(BaseModel, DBCaseConfig):
    """Per-case index and search settings for a dense vector field."""

    # Set by Assembler.assemble from the case's dataset; every non-empty
    # DBCaseConfig must accept it. It decides `space_type`, because the metric
    # has to match the ground truth the recall is scored against - and because
    # EndeeLib.need_normalize_cosine() keys off the space type, so a mismatch
    # would leave cosine vectors un-normalized.
    metric_type: MetricType | None = None

    space_type: str = "cosine"
    precision: str = "int16"
    M: int = 16
    ef_con: int = 128

    ef_search: int = 128

    # Capacity is provisioned at create time; the engine cannot grow later.
    max_elements: int | None = None
    size_in_millions: int | None = None

    # Filtered-search tuning (passed through as search filter_params).
    prefilter_threshold: float | None = None
    boost_percentage: float | None = None

    def parse_space_type(self) -> str:
        """The engine space type for this case.

        The dataset's metric wins when the framework has reported one: scoring
        recall against cosine ground truth with an l2 index (or vice versa) only
        produces a wrong number. An unsupported metric falls back to the
        configured `space_type`.
        """
        if self.metric_type is None:
            return self.space_type
        derived = _SPACE_TYPE_FOR_METRIC.get(self.metric_type)
        if derived is None:
            log.warning(
                f"EndeeLib: dataset metric {self.metric_type} has no engine space type; "
                f"using --space-type {self.space_type}"
            )
            return self.space_type
        if derived != self.space_type:
            log.warning(
                f"EndeeLib: dataset metric is {self.metric_type}, so using space_type "
                f"{derived!r} instead of the configured {self.space_type!r}"
            )
        return derived

    def index_param(self) -> dict:
        return {
            "space_type": self.parse_space_type(),
            "precision": self.precision,
            "M": self.M,
            "ef_con": self.ef_con,
        }

    def search_param(self) -> dict:
        params: dict = {"ef_search": self.ef_search}
        if self.prefilter_threshold is not None:
            params["prefilter_threshold"] = self.prefilter_threshold
        if self.boost_percentage is not None:
            params["boost_percentage"] = self.boost_percentage
        return params
