import logging
from typing import Annotated, Unpack

import click

from vectordb_bench.backend.clients import DB
from vectordb_bench.cli.cli import (
    CommonTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)

log = logging.getLogger(__name__)


class EndeeLibTypedDict(CommonTypedDict):
    data_dir: Annotated[
        str,
        click.option("--data-dir", type=str, default="./endee_data", help="Engine data directory", show_default=True),
    ]
    db_namespace: Annotated[
        str,
        click.option(
            "--db-namespace",
            type=str,
            default="default",
            help="Namespace within the data directory",
            show_default=True,
        ),
    ]
    collection_name: Annotated[
        str,
        click.option("--collection-name", type=str, default="endee_bench", help="Collection name", show_default=True),
    ]
    space_type: Annotated[
        str,
        click.option(
            "--space-type",
            type=click.Choice(["cosine", "l2", "ip"]),
            default="cosine",
            help="Distance metric",
            show_default=True,
        ),
    ]
    precision: Annotated[
        str,
        click.option(
            "--precision",
            type=click.Choice(["binary", "int8", "int8e", "int16", "float16", "float32"]),
            default="int16",
            help="Vector quantization level",
            show_default=True,
        ),
    ]
    m: Annotated[int, click.option("--m", type=int, default=16, help="HNSW M parameter", show_default=True)]
    ef_con: Annotated[
        int, click.option("--ef-con", type=int, default=128, help="HNSW construction parameter", show_default=True)
    ]
    ef_search: Annotated[
        int, click.option("--ef-search", type=int, default=128, help="HNSW search parameter", show_default=True)
    ]
    max_elements: Annotated[
        int | None,
        click.option(
            "--max-elements",
            type=int,
            default=None,
            help="Capacity provisioned at create time (fixed thereafter)",
        ),
    ]
    size_in_millions: Annotated[
        int | None,
        click.option(
            "--size-in-millions",
            type=int,
            default=None,
            help="Capacity in millions of elements; overrides --max-elements (1-10000)",
        ),
    ]
    prefilter_threshold: Annotated[
        float | None,
        click.option(
            "--prefilter-threshold",
            type=float,
            default=None,
            help="Use brute-force prefiltering when the filter matches at most N vectors",
        ),
    ]
    boost_percentage: Annotated[
        float | None,
        click.option(
            "--boost-percentage",
            type=float,
            default=None,
            help="Increase the search limit to offset filtered-out results (0-100)",
        ),
    ]
    skip_sanity: Annotated[
        bool,
        click.option("--skip-sanity/--no-skip-sanity", default=False, help="Skip engine startup disk checks"),
    ]
    save_on_shutdown: Annotated[
        bool,
        click.option("--save-on-shutdown/--no-save-on-shutdown", default=True, help="Flush collections on close"),
    ]
    vector_cache_max_bytes: Annotated[
        int,
        click.option("--vector-cache-max-bytes", type=int, default=0, help="Vector cache budget (0 = engine default)"),
    ]
    parallel_insert_threads: Annotated[
        int,
        click.option("--parallel-insert-threads", type=int, default=0, help="Insert threads (0 = engine default)"),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(EndeeLibTypedDict)
def EndeeLib(**parameters: Unpack[EndeeLibTypedDict]):
    """Run VectorDBBench against the embedded Endee engine (nddlib)."""
    from .config import EndeeLibConfig, EndeeLibIndexConfig

    run(
        db=DB.EndeeLib,
        db_config=EndeeLibConfig(
            db_label=parameters["db_label"],
            data_dir=parameters["data_dir"],
            db=parameters["db_namespace"],
            collection_name=parameters["collection_name"],
            skip_sanity=parameters["skip_sanity"],
            save_on_shutdown=parameters["save_on_shutdown"],
            vector_cache_max_bytes=parameters["vector_cache_max_bytes"],
            parallel_insert_threads=parameters["parallel_insert_threads"],
        ),
        db_case_config=EndeeLibIndexConfig(
            space_type=parameters["space_type"],
            precision=parameters["precision"],
            M=parameters["m"],
            ef_con=parameters["ef_con"],
            ef_search=parameters["ef_search"],
            max_elements=parameters["max_elements"],
            size_in_millions=parameters["size_in_millions"],
            prefilter_threshold=parameters["prefilter_threshold"],
            boost_percentage=parameters["boost_percentage"],
        ),
        **parameters,
    )
