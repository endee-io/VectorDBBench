from typing import Annotated, Unpack

import click
from pydantic import SecretStr

from vectordb_bench.backend.clients import DB
from vectordb_bench.cli.cli import (
    CommonTypedDict,
    HNSWFlavor1,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)


class VespaTypedDict(CommonTypedDict, HNSWFlavor1):
    uri: Annotated[
        str,
        click.option("--uri", "-u", type=str, help="uri connection string", default="http://127.0.0.1"),
    ]
    port: Annotated[
        int,
        click.option("--port", "-p", type=int, help="connection port", default=8080),
    ]
    config_port: Annotated[
        int,
        click.option("--config-port", type=int, help="Vespa config-server (deploy) port", default=19071, show_default=True),
    ]
    collection_name: Annotated[
        str,
        click.option(
            "--collection-name",
            type=str,
            default=None,
            help="Vespa schema/document-type name (letters, digits, _; default: VectorDBBenchCollection)",
        ),
    ]
    quantization: Annotated[
        str,
        click.option(
            "--quantization",
            type=click.Choice(["none", "binary", "bfloat16", "int8"], case_sensitive=False),
            default="none",
        ),
    ]
    int8_max_abs: Annotated[
        float | None,
        click.option(
            "--int8-max-abs",
            type=float,
            default=None,
            help=(
                "Largest absolute component in the dataset, required by --quantization int8. "
                "Measure with `python -m vectordb_bench.tools.int8_scale <train.parquet>`."
            ),
        ),
    ]
    label_field: Annotated[
        bool,
        click.option(
            "--label-field/--no-label-field",
            default=False,
            help="Keep the label attribute in the schema even for cases without labels (search-only int-filter runs on a "
            "collection loaded by a label-filter case)",
        ),
    ]
    approximate_threshold: Annotated[
        float | None,
        click.option("--approximate-threshold", type=float, default=None,
                     help="Query-time ranking.matching.approximateThreshold (default: Vespa's)"),
    ]
    filter_first_threshold: Annotated[
        float | None,
        click.option("--filter-first-threshold", type=float, default=None,
                     help="Query-time ranking.matching.filterFirstThreshold (default: Vespa's)"),
    ]
    filter_first_exploration: Annotated[
        float | None,
        click.option("--filter-first-exploration", type=float, default=None,
                     help="Query-time ranking.matching.filterFirstExploration (default: Vespa's)"),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(VespaTypedDict)
def Vespa(**params: Unpack[VespaTypedDict]):
    from .config import VespaConfig, VespaHNSWConfig

    case_params = {
        "quantization_type": params["quantization"],
        "M": params["m"],
        "efConstruction": params["ef_construction"],
        "ef": params["ef_search"],
        "int8_max_abs": params["int8_max_abs"],
    }

    for key in ("approximate_threshold", "filter_first_threshold", "filter_first_exploration"):
        if params[key] is not None:  # 0 is a valid threshold, so not filtered by truthiness below
            case_params[key] = params[key]
    case_params["label_field"] = params["label_field"]

    run(
        db=DB.Vespa,
        db_config=VespaConfig(url=SecretStr(params["uri"]), port=params["port"], config_port=params["config_port"], collection_name=params["collection_name"]),
        db_case_config=VespaHNSWConfig(
            **{k: v for k, v in case_params.items() if v or (k in ("approximate_threshold", "filter_first_threshold", "filter_first_exploration") and v is not None)}
        ),
        **params,
    )
