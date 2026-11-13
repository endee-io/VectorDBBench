from typing import Literal

from pydantic import BaseModel, SecretStr

from ..api import DBCaseConfig, DBConfig, MetricType


class QdrantLocalConfig(DBConfig):
    url: SecretStr
    # REST by default (unchanged). gRPC (port 6334) avoids JSON encoding of float vectors
    # and is what Qdrant recommends for high-throughput clients.
    prefer_grpc: bool = False
    collection_name: str | None = None  # None -> client default ("QdrantLocalCollection")
    # REST only: ask Qdrant for uncompressed replies. With compression (the client's default) replies of ~1 KB or more
    # (k >= ~30) stalled ~42 ms per request on our network (TCP delayed-ACK on the last gzip chunk); results are identical.
    rest_compression: bool = True

    def to_dict(self) -> dict:
        d = {"url": self.url.get_secret_value()}
        if self.collection_name:
            d["collection_name"] = self.collection_name
        if not self.rest_compression and not self.prefer_grpc:
            d["metadata"] = {"Accept-Encoding": "identity"}  # sent as an HTTP header by qdrant-client's REST transport
        if self.prefer_grpc:
            d["prefer_grpc"] = True
        return d


# Quantization modes supported by Qdrant (lib/segment/src/types.rs, v1.20):
#   scalar  -> int8 (4x), optional quantile
#   product -> x4 / x8 / x16 / x32 / x64 compression
#   binary  -> 1 bit, 1.5 bit, 2 bit encodings
#   turbo   -> TurboQuant 1 / 1.5 / 2 / 4 / 8 bits
QdrantQuantization = Literal[
    "none",
    "scalar",
    "product_x4",
    "product_x8",
    "product_x16",
    "product_x32",
    "product_x64",
    "binary",
    "binary_1_5bit",
    "binary_2bit",
    "turbo_1bit",
    "turbo_1_5bit",
    "turbo_2bit",
    "turbo_4bit",
    "turbo_8bit",
]
# Stored vector datatype. uint8 needs pre-quantised input so is not exposed;
# turbo4/8/16 are TurboQuant compressed *storage* datatypes.
QdrantDatatype = Literal["float32", "float16", "turbo4", "turbo8", "turbo16"]


class QdrantLocalIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None
    m: int
    ef_construct: int
    hnsw_ef: int | None = 0
    # ACORN filtered search (query-time, Qdrant >= 1.16): also explores neighbours of filtered-out neighbours.
    # Only used when the estimated filter selectivity is below acorn_max_selectivity (Qdrant default 0.4).
    acorn: bool = False
    acorn_max_selectivity: float | None = None
    on_disk: bool | None = False
    quantization: QdrantQuantization = "none"
    datatype: QdrantDatatype = "float32"
    quant_always_ram: bool = True
    quant_quantile: float | None = None  # scalar only, range [0.5, 1.0]
    quant_rescore: bool = True  # re-score top candidates with original vectors
    quant_oversampling: float | None = None  # e.g. 2.0; None -> server default
    binary_query_encoding: Literal["default", "binary", "scalar4bits", "scalar8bits"] = "default"

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "Euclid"

        if self.metric_type == MetricType.IP:
            return "Dot"

        return "Cosine"

    def index_param(self) -> dict:
        return {
            "distance": self.parse_metric(),
            "m": self.m,
            "ef_construct": self.ef_construct,
            "on_disk": self.on_disk,
            "quantization": self.quantization,
            "datatype": self.datatype,
        }

    def quantization_config(self):
        """Build the qdrant-client quantization config (None when disabled)."""
        from qdrant_client.http import models as qm

        q = self.quantization
        if q == "none":
            return None
        if q == "scalar":
            return qm.ScalarQuantization(
                scalar=qm.ScalarQuantizationConfig(
                    type=qm.ScalarType.INT8, quantile=self.quant_quantile, always_ram=self.quant_always_ram
                )
            )
        if q.startswith("product_"):
            ratio = getattr(qm.CompressionRatio, q.removeprefix("product_").upper())
            return qm.ProductQuantization(
                product=qm.ProductQuantizationConfig(compression=ratio, always_ram=self.quant_always_ram)
            )
        if q.startswith("binary"):
            encoding = {
                "binary": qm.BinaryQuantizationEncoding.ONE_BIT,
                "binary_1_5bit": qm.BinaryQuantizationEncoding.ONE_AND_HALF_BITS,
                "binary_2bit": qm.BinaryQuantizationEncoding.TWO_BITS,
            }[q]
            return qm.BinaryQuantization(
                binary=qm.BinaryQuantizationConfig(
                    always_ram=self.quant_always_ram,
                    encoding=encoding,
                    query_encoding=qm.BinaryQuantizationQueryEncoding(self.binary_query_encoding),
                )
            )
        if q.startswith("turbo_"):
            bits = getattr(qm.TurboQuantBitSize, "BITS" + q.removeprefix("turbo_").removesuffix("bit").upper())
            return qm.TurboQuantization(
                turbo=qm.TurboQuantQuantizationConfig(always_ram=self.quant_always_ram, bits=bits)
            )
        raise NotImplementedError(q)

    def search_param(self) -> dict:
        search_params = {
            "exact": False,  # Force to use ANNs
        }

        if self.hnsw_ef != 0:
            search_params["hnsw_ef"] = self.hnsw_ef

        if self.acorn:
            acorn = {"enable": True}
            if self.acorn_max_selectivity is not None:
                acorn["max_selectivity"] = self.acorn_max_selectivity
            search_params["acorn"] = acorn

        if self.quantization != "none":
            search_params["quantization"] = {
                "ignore": False,
                "rescore": self.quant_rescore,
                "oversampling": self.quant_oversampling,
            }

        return search_params
