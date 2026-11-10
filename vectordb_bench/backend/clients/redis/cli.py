from typing import Annotated, TypedDict, Unpack

import click
from pydantic import SecretStr

from ....cli.cli import (
    CommonTypedDict,
    HNSWFlavor2,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)
from .. import DB
from .config import RedisHNSWConfig


class RedisTypedDict(TypedDict):
    host: Annotated[str, click.option("--host", type=str, help="Db host", required=True)]
    password: Annotated[str, click.option("--password", type=str, help="Db password")]
    port: Annotated[int, click.option("--port", type=int, default=6379, help="Db Port")]
    ssl: Annotated[
        bool,
        click.option(
            "--ssl/--no-ssl",
            is_flag=True,
            show_default=True,
            default=True,
            help="Enable or disable SSL for Redis",
        ),
    ]
    ssl_ca_certs: Annotated[
        str,
        click.option(
            "--ssl-ca-certs",
            show_default=True,
            help="Path to certificate authority file to use for SSL",
        ),
    ]
    cmd: Annotated[
        bool,
        click.option(
            "--cmd",
            is_flag=True,
            show_default=True,
            default=False,
            help="Cluster Mode Disabled (CMD) for Redis doesn't use Cluster conn",
        ),
    ]


class RedisHNSWTypedDict(CommonTypedDict, RedisTypedDict, HNSWFlavor2):
    collection_name: Annotated[
        str,
        click.option("--collection-name", type=str, default=None, help="Index name (default: index)"),
    ]
    vector_type: Annotated[
        str,
        click.option(
            "--vector-type",
            type=click.Choice(["FLOAT32", "FLOAT16", "BFLOAT16"], case_sensitive=False),
            default="FLOAT32",
            show_default=True,
            help="Vector field storage type",
        ),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(RedisHNSWTypedDict)
def Redis(**parameters: Unpack[RedisHNSWTypedDict]):
    from .config import RedisConfig

    run(
        db=DB.Redis,
        db_config=RedisConfig(
            db_label=parameters["db_label"],
            password=SecretStr(parameters["password"]) if parameters["password"] else None,
            host=SecretStr(parameters["host"]),
            port=parameters["port"],
            ssl=parameters["ssl"],
            ssl_ca_certs=parameters["ssl_ca_certs"],
            cmd=parameters["cmd"],
            collection_name=parameters["collection_name"],
        ),
        db_case_config=RedisHNSWConfig(
            M=parameters["m"],
            efConstruction=parameters["ef_construction"],
            ef=parameters["ef_runtime"],
            vector_type=parameters["vector_type"].upper(),
        ),
        **parameters,
    )
