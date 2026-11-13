from typing import Annotated, Unpack, get_args

import click
from pydantic import SecretStr

from vectordb_bench.backend.clients import DB
from vectordb_bench.cli.cli import (
    CommonTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)

from .config import QdrantDatatype, QdrantQuantization

DBTYPE = DB.QdrantLocal


class QdrantLocalTypedDict(CommonTypedDict):
    url: Annotated[
        str,
        click.option("--url", type=str, help="Qdrant url", required=True),
    ]
    collection_name: Annotated[
        str,
        click.option("--collection-name", type=str, default=None, help="Collection name (default: QdrantLocalCollection)"),
    ]
    rest_compression: Annotated[
        bool,
        click.option(
            "--rest-compression/--no-rest-compression",
            default=True,
            show_default=True,
            help="Let Qdrant gzip REST replies (client default). --no-rest-compression avoids a ~42 ms TCP stall on some networks",
        ),
    ]
    prefer_grpc: Annotated[
        bool,
        click.option("--prefer-grpc", type=bool, default=False, show_default=True, help="Use gRPC (port 6334) instead of REST"),
    ]
    on_disk: Annotated[
        bool,
        click.option("--on-disk", type=bool, default=False, help="Store the vectors and the HNSW index on disk"),
    ]
    m: Annotated[
        int,
        click.option("--m", type=int, default=16, help="HNSW index parameter m, set 0 to disable the index"),
    ]
    ef_construct: Annotated[
        int,
        click.option("--ef-construct", type=int, default=200, help="HNSW index parameter ef_construct"),
    ]
    quantization: Annotated[
        str,
        click.option(
            "--quantization",
            type=click.Choice(list(get_args(QdrantQuantization)), case_sensitive=False),
            default="none",
            show_default=True,
            help="Qdrant quantization: scalar(int8) / product_xN / binary(1,1.5,2 bit) / turbo(1..8 bit)",
        ),
    ]
    datatype: Annotated[
        str,
        click.option(
            "--datatype",
            type=click.Choice(list(get_args(QdrantDatatype)), case_sensitive=False),
            default="float32",
            show_default=True,
            help="Stored vector datatype",
        ),
    ]
    quant_always_ram: Annotated[
        bool,
        click.option("--quant-always-ram", type=bool, default=True, show_default=True, help="Keep quantized vectors in RAM"),
    ]
    quant_quantile: Annotated[
        float,
        click.option("--quant-quantile", type=float, default=None, help="Scalar quantile in [0.5, 1.0]"),
    ]
    quant_rescore: Annotated[
        bool,
        click.option("--quant-rescore", type=bool, default=True, show_default=True, help="Rescore with original vectors"),
    ]
    quant_oversampling: Annotated[
        float,
        click.option("--quant-oversampling", type=float, default=None, help="Oversampling factor for quantized search"),
    ]
    binary_query_encoding: Annotated[
        str,
        click.option(
            "--binary-query-encoding",
            type=click.Choice(["default", "binary", "scalar4bits", "scalar8bits"]),
            default="default",
            show_default=True,
            help="Query encoding for binary quantization",
        ),
    ]
    hnsw_ef: Annotated[
        int,
        click.option(
            "--hnsw-ef",
            type=int,
            default=0,
            help="HNSW index parameter hnsw_ef, set 0 to use ef_construct for search",
        ),
    ]
    acorn: Annotated[
        bool,
        click.option("--acorn/--no-acorn", default=False, show_default=True,
                     help="Query-time ACORN filtered search (Qdrant >= 1.16)"),
    ]
    acorn_max_selectivity: Annotated[
        float | None,
        click.option("--acorn-max-selectivity", type=float, default=None,
                     help="ACORN max_selectivity (Qdrant default 0.4: ACORN only for filters keeping < 40 %)"),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(QdrantLocalTypedDict)
def QdrantLocal(**parameters: Unpack[QdrantLocalTypedDict]):
    from .config import QdrantLocalConfig, QdrantLocalIndexConfig

    run(
        db=DBTYPE,
        db_config=QdrantLocalConfig(url=SecretStr(parameters["url"]), prefer_grpc=parameters["prefer_grpc"], collection_name=parameters["collection_name"],
            rest_compression=parameters["rest_compression"]),
        db_case_config=QdrantLocalIndexConfig(
            on_disk=parameters["on_disk"],
            m=parameters["m"],
            ef_construct=parameters["ef_construct"],
            hnsw_ef=parameters["hnsw_ef"],
            acorn=parameters["acorn"],
            acorn_max_selectivity=parameters["acorn_max_selectivity"],
            quantization=parameters["quantization"].lower(),
            datatype=parameters["datatype"].lower(),
            quant_always_ram=parameters["quant_always_ram"],
            quant_quantile=parameters["quant_quantile"],
            quant_rescore=parameters["quant_rescore"],
            quant_oversampling=parameters["quant_oversampling"],
            binary_query_encoding=parameters["binary_query_encoding"],
        ),
        **parameters,
    )
