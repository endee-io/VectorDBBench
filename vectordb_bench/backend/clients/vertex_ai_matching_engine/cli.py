import logging
import time
import uuid
from concurrent.futures import wait
from typing import Annotated

import click

from vectordb_bench.cli.cli import (
    CommonTypedDict,
    benchmark_runner,
    click_parameter_decorators_from_typed_dict,
    get_custom_case_config,
)
from vectordb_bench.models import TaskStage
from vectordb_bench.models import (
    CaseConfig,
    CaseType,
    ConcurrencySearchConfig,
    TaskConfig,
)

from .. import DB
from ..api import MetricType
from .config import VertexAIMatchingEngineConfig, VertexAIMatchingEngineIndexConfig

log = logging.getLogger(__name__)


class VertexAIMatchingEngineTypedDict(CommonTypedDict):
    project: Annotated[
        str,
        click.option("--project", type=str, required=True, help="GCP project ID"),
    ]
    location: Annotated[
        str,
        click.option(
            "--location",
            type=str,
            default="us-central1",
            show_default=True,
            help="GCP region",
        ),
    ]
    index_display_name: Annotated[
        str,
        click.option(
            "--index-display-name",
            type=str,
            default="vectordbbench-index",
            show_default=True,
            help="Display name for the MatchingEngineIndex resource",
        ),
    ]
    endpoint_display_name: Annotated[
        str,
        click.option(
            "--endpoint-display-name",
            type=str,
            default="vectordbbench-endpoint",
            show_default=True,
            help="Display name for the IndexEndpoint resource",
        ),
    ]
    deployed_index_id: Annotated[
        str,
        click.option(
            "--deployed-index-id",
            type=str,
            default="vectordbbench_deployed",
            show_default=True,
            help="ID used when deploying the index to the endpoint (a-z, 0-9, _)",
        ),
    ]
    machine_type: Annotated[
        str | None,
        click.option(
            "--machine-type",
            type=str,
            default=None,
            show_default=True,
            help="VM type for endpoint replicas (e.g. n1-standard-16); None = API default",
        ),
    ]
    min_replica_count: Annotated[
        int,
        click.option(
            "--min-replica-count",
            type=int,
            default=1,
            show_default=True,
            help="Minimum number of endpoint replicas",
        ),
    ]
    max_replica_count: Annotated[
        int,
        click.option(
            "--max-replica-count",
            type=int,
            default=1,
            show_default=True,
            help="Maximum number of endpoint replicas",
        ),
    ]
    shard_size: Annotated[
        str,
        click.option(
            "--shard-size",
            type=click.Choice(["SHARD_SIZE_SMALL", "SHARD_SIZE_MEDIUM", "SHARD_SIZE_LARGE"]),
            default="SHARD_SIZE_SMALL",
            show_default=True,
            help="Index shard size: SMALL=≤2GB (e2-standard-2), MEDIUM=≤10GB (n1-standard-16), LARGE=≤50GB",
        ),
    ]
    gcs_bucket: Annotated[
        str | None,
        click.option(
            "--gcs-bucket",
            type=str,
            default=None,
            show_default=True,
            help="GCS bucket name for BATCH_UPDATE index uploads (e.g. my-bucket)",
        ),
    ]
    reuse_gcs_folder: Annotated[
        str | None,
        click.option(
            "--reuse-gcs-folder",
            type=str,
            default=None,
            show_default=True,
            help="Skip upload and build index from this existing GCS folder (e.g. vectordbbench/vdb-batch-index-1m)",
        ),
    ]
    metric_type: Annotated[
        str,
        click.option(
            "--metric-type",
            type=click.Choice(["COSINE", "L2", "IP"]),
            default="COSINE",
            show_default=True,
            help="Distance metric for the ScaNN index",
        ),
    ]
    approximate_neighbors_count: Annotated[
        int,
        click.option(
            "--approximate-neighbors-count",
            type=int,
            default=128,
            show_default=True,
            help="Neighbors per leaf considered at query time (recall vs. speed tradeoff)",
        ),
    ]
    leaf_node_embedding_count: Annotated[
        int,
        click.option(
            "--leaf-node-embedding-count",
            type=int,
            default=1000,
            show_default=True,
            help="Embeddings per leaf node (tree branching factor)",
        ),
    ]
    leaf_nodes_to_search_percent: Annotated[
        int,
        click.option(
            "--leaf-nodes-to-search-percent",
            type=int,
            default=5,
            show_default=True,
            help="Percentage of leaf nodes searched per query (1–100)",
        ),
    ]
    fraction_leaf_nodes_to_search_override: Annotated[
        float | None,
        click.option(
            "--fraction-leaf-nodes-to-search-override",
            type=float,
            default=None,
            show_default=True,
            help="Per-query fraction override (0.0–1.0); omit to use index default",
        ),
    ]


@click.command(name="vertex_ai_matching_engine")
@click_parameter_decorators_from_typed_dict(VertexAIMatchingEngineTypedDict)
def VertexAIMatchingEngine(**parameters):
    """Run VectorDBBench against Vertex AI Vector Search (Matching Engine / tree-AH)."""
    # Build stages manually — upsert_datapoints is idempotent so --skip-drop-old
    # with --load is valid here, unlike the default framework restriction.
    stages = []
    if parameters["drop_old"]:
        stages.append(TaskStage.DROP_OLD)
    if parameters["load"]:
        stages.append(TaskStage.LOAD)
    if parameters["search_serial"]:
        stages.append(TaskStage.SEARCH_SERIAL)
    if parameters["search_concurrent"]:
        stages.append(TaskStage.SEARCH_CONCURRENT)
    if not stages:
        raise RuntimeError("Must specify at least one of --drop-old, --load, --search-serial, --search-concurrent")

    db_config = VertexAIMatchingEngineConfig(
        db_label=parameters.get("db_label", ""),
        project=parameters["project"],
        location=parameters["location"],
        index_display_name=parameters["index_display_name"],
        endpoint_display_name=parameters["endpoint_display_name"],
        deployed_index_id=parameters["deployed_index_id"],
        machine_type=parameters["machine_type"],
        min_replica_count=parameters["min_replica_count"],
        max_replica_count=parameters["max_replica_count"],
        shard_size=parameters["shard_size"],
        gcs_bucket=parameters["gcs_bucket"],
        reuse_gcs_folder=parameters["reuse_gcs_folder"],
    )

    db_case_config = VertexAIMatchingEngineIndexConfig(
        metric_type=MetricType(parameters["metric_type"]),
        approximate_neighbors_count=parameters["approximate_neighbors_count"],
        leaf_node_embedding_count=parameters["leaf_node_embedding_count"],
        leaf_nodes_to_search_percent=parameters["leaf_nodes_to_search_percent"],
        fraction_leaf_nodes_to_search_override=parameters["fraction_leaf_nodes_to_search_override"],
    )

    custom_case_config = get_custom_case_config(parameters)

    task = TaskConfig(
        db=DB.VertexAIMatchingEngine,
        db_config=db_config,
        db_case_config=db_case_config,
        case_config=CaseConfig(
            case_id=CaseType[parameters["case_type"]],
            k=parameters["k"],
            concurrency_search_config=ConcurrencySearchConfig(
                concurrency_duration=parameters["concurrency_duration"],
                num_concurrency=[int(s) for s in parameters["num_concurrency"]],
                concurrency_timeout=parameters["concurrency_timeout"],
            ),
            custom_case=custom_case_config,
        ),
        stages=stages,
    )

    if parameters["dry_run"]:
        return

    run_label = parameters.get("task_label") or "vertex_matching_engine"
    benchmark_runner.run([task], f"{run_label}_{uuid.uuid4().hex}")

    from vectordb_bench.interface import global_result_future

    time.sleep(5)
    if global_result_future:
        wait([global_result_future])

    while benchmark_runner.has_running():
        time.sleep(1)
