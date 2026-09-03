from pydantic import BaseModel

from vectordb_bench.backend.clients.api import DBCaseConfig, DBConfig


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

    def index_param(self) -> dict:
        return {
            "space_type": self.space_type,
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
