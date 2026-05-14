from typing import Annotated, Unpack
import click

from ....cli.cli import (
    CommonTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)
from .. import DB
from ..cosmos.config import CosmosDBConfig, CosmosDBIndexConfig


class CosmosDBTypedDict(CommonTypedDict):
    # VectorDBBench requires parameters to be annotated with click.option
    endpoint: Annotated[
        str, 
        click.option("--endpoint", required=True, help="Azure Cosmos DB Endpoint URI")
    ]
    key: Annotated[
        str, 
        click.option("--key", required=True, help="Azure Cosmos DB Primary Key")
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(CosmosDBTypedDict)
def CosmosDB(**parameters: Unpack[CosmosDBTypedDict]):
    run(
        db=DB.Cosmos,
        db_config=CosmosDBConfig(
            db_label=parameters["db_label"],
            endpoint=parameters["endpoint"],
            key=parameters["key"]
        ),
        db_case_config=CosmosDBIndexConfig(),
        **parameters,
    )