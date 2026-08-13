from pydantic import SecretStr

from vectordb_bench.backend.clients.api import DBConfig


class EndeeConfig(DBConfig):
    token: SecretStr | None = None
    region: str | None = None
    base_url: str = "http://127.0.0.1:8080/api/v2"
    space_type: str = "cosine"
    precision: str = "int8"
    version: str | None = None
    m: int | None = 16
    ef_con: int | None = 128
    ef_search: int | None = 128
    collection_name: str
    prefilter_cardinality_threshold: int | None = 10000
    filter_boost_percentage: int | None = 0
    field_type: str = "dense"
    multivec_fields: list[str] = ["multivec"]
    multivec_pooling: str = "mean"
    rrf_k: int = 60
    field_weights: dict | None = None
    multivec_count: int = 1
    search_field: str | None = None

    def to_dict(self) -> dict:
        return {
            "token": self.token.get_secret_value() if self.token else None,
            "region": self.region,
            "base_url": self.base_url,
            "space_type": self.space_type,
            "precision": self.precision,
            "version": self.version,
            "m": self.m,
            "ef_con": self.ef_con,
            "ef_search": self.ef_search,
            "collection_name": self.collection_name,
            "prefilter_cardinality_threshold": self.prefilter_cardinality_threshold,
            "filter_boost_percentage": self.filter_boost_percentage,
            "field_type": self.field_type,
            "multivec_fields": self.multivec_fields,
            "multivec_pooling": self.multivec_pooling,
            "rrf_k": self.rrf_k,
            "field_weights": self.field_weights,
            "multivec_count": self.multivec_count,
            "search_field": self.search_field,
        }
