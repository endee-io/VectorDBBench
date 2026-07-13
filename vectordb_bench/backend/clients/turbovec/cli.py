from typing import Annotated, Unpack

import click

from ....cli.cli import (
    CommonTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)
from .. import DB
from .config import DEFAULT_TURBOVEC_PATH


class TurboVecTypedDict(CommonTypedDict):
    bit_width: Annotated[
        int,
        click.option(
            "--bit-width",
            type=int,
            default=4,
            help="Quantization bit width for the TurboQuant algorithm (2 or 4)",
            show_default=True,
        ),
    ]
    path: Annotated[
        str,
        click.option(
            "--path",
            type=str,
            default=DEFAULT_TURBOVEC_PATH,
            help="Directory to persist the turbovec .tvim index file in. turbovec is "
            "embedded/in-process (no server), so this directory stands in for a DB "
            "connection across VectorDBBench's per-phase subprocesses.",
            show_default=True,
        ),
    ]
    collection_name: Annotated[
        str,
        click.option(
            "--collection-name",
            type=str,
            default="turbovec_bench",
            help="Name for this index, used as the persisted <path>/<collection-name>.tvim "
            "filename. Use a different name per run to keep runs from overwriting each "
            "other's persisted index.",
            show_default=True,
        ),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(TurboVecTypedDict)
def TurboVec(**parameters: Unpack[TurboVecTypedDict]):
    """Run VectorDBBench performance tests on turbovec (embedded TurboQuant index)."""
    from .config import TurboVecConfig, TurboVecIndexConfig

    run(
        db=DB.TurboVec,
        db_config=TurboVecConfig(
            db_label=parameters["db_label"],
            bit_width=parameters["bit_width"],
            path=parameters["path"],
            collection_name=parameters["collection_name"],
        ),
        db_case_config=TurboVecIndexConfig(),
        **parameters,
    )
