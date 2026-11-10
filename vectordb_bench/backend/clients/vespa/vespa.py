import datetime
import logging
import io
import math
import re
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Generator
from contextlib import contextmanager

import numpy as np
from vespa import application

from ..api import VectorDB
from . import util
from .config import VespaHNSWConfig

log = logging.getLogger(__name__)


class Vespa(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict[str, str],
        db_case_config: VespaHNSWConfig | None = None,
        collection_name: str = "VectorDBBenchCollection",
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.dim = dim
        self.db_config = db_config
        self.case_config = db_case_config or VespaHNSWConfig()
        # Vespa schema / document-type names must look like identifiers.
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", collection_name or ""):
            msg = f"invalid Vespa schema name {collection_name!r}: use letters, digits and underscores, starting with a letter"
            raise ValueError(msg)
        self.schema_name = collection_name
        # Pooled query session; opened per process in init(), see the note there.
        self.sync = None

        client = self.deploy_http()
        client.wait_for_application_up()

        if drop_old:
            try:
                client.delete_all_docs("vectordbbench_content", self.schema_name)
            except Exception:
                drop_old = False
                log.exception(f"Vespa client drop_old schema: {self.schema_name}")

    @contextmanager
    def init(self) -> Generator[None, None, None]:
        """create and destory connections to database.
        Why contextmanager:

            In multiprocessing search tasks, vectordbbench might init
            totally hundreds of thousands of connections with DB server.

            Too many connections may drain local FDs or server connection resources.
            If the DB client doesn't have `close()` method, just set the object to None.

        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
        """
        self.client = application.Vespa(self.db_config["url"], port=self.db_config["port"])
        # Vespa.query() wraps every single call in its own short-lived VespaSync
        # ("Use one connection as this is a single query"), so searching through it
        # opens and closes a TCP connection per query. At benchmark rates that
        # exhausts the client's ephemeral ports -- the sockets pile up in TIME_WAIT
        # -- and the measurement becomes connection setup rather than Vespa. Hold
        # one pooled session open for the life of the process instead; feeding
        # already pools internally, so only search needs this.
        with self.client.syncio(connections=10) as sync:
            self.sync = sync
            yield
            self.sync = None
        self.client = None

    def need_normalize_cosine(self) -> bool:
        """Wheather this database need to normalize dataset to support COSINE"""
        return False

    def _quantize_int8(self, vectors: list) -> list:
        """Map float vectors onto the int8 grid using the corpus-derived scale.

        Applied identically to documents and queries, since a cosine comparison
        is only meaningful when both sides live in the same space. Rounds rather
        than truncates, and clamps as a backstop so a component larger than the
        measured maximum saturates at +-127 instead of wrapping to the opposite
        sign.
        """
        scaled = np.asarray(vectors, dtype=np.float32) * self.case_config.int8_scale
        return np.clip(np.rint(scaled), -127, 127).astype(np.int8).tolist()

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs,
    ) -> tuple[int, Exception | None]:
        """Insert the embeddings to the vector database. The default number of embeddings for
        each insert_embeddings is 5000.

        Args:
            embeddings(list[list[float]]): list of embedding to add to the vector database.
            metadatas(list[int]): metadata associated with the embeddings, for filtering.
            **kwargs(Any): vector database specific parameters.

        Returns:
            int: inserted data count
        """
        assert self.client is not None

        if self.case_config.quantization_type == "int8":
            embeddings = self._quantize_int8(embeddings)

        data = ({"id": str(i), "fields": {"id": i, "embedding": e}} for i, e in zip(metadata, embeddings, strict=True))
        self.client.feed_iterable(data, self.schema_name)
        return len(embeddings), None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
    ) -> list[int]:
        """Get k most similar embeddings to query vector.

        Args:
            query(list[float]): query embedding to look up documents similar to.
            k(int): Number of most similar embeddings to return. Defaults to 100.
            filters(dict, optional): filtering expression to filter the data while searching.

        Returns:
            list[int]: list of k most similar embeddings IDs to the query embedding.
        """
        assert self.sync is not None

        ef = self.case_config.ef
        extra_ef = max(0, ef - k)
        # Only the binary mode indexes a separate derived field; bfloat16 and int8
        # change the cell type of `embedding` itself.
        embedding_field = "embedding_binary" if self.case_config.quantization_type == "binary" else "embedding"

        yql = (
            f"select id from {self.schema_name} where "
            f"{{targetHits: {k}, hnsw.exploreAdditionalHits: {extra_ef}}}"
            f"nearestNeighbor({embedding_field}, query_embedding)"
        )

        if filters:
            id_filter = filters.get("id")
            yql += f" and id >= {id_filter}"

        if self.case_config.quantization_type == "binary":
            query_embedding = util.binarize_tensor(query)
        elif self.case_config.quantization_type == "int8":
            query_embedding = self._quantize_int8(query)
        else:
            query_embedding = query

        ranking = self.case_config.quantization_type

        # `hits` defaults to 10 in Vespa, which would cap recall whenever k > 10.
        # Goes through the pooled session from init(), not self.client.query().
        result = self.sync.query(
            {
                "yql": yql,
                "hits": k,
                "input.query(query_embedding)": query_embedding,
                "ranking": ranking,
            }
        )
        return [child["fields"]["id"] for child in result.get_json()["root"]["children"]]

    def optimize(self, data_size: int | None = None):
        """optimize will be called between insertion and search in performance cases.

        Should be blocked until the vectorDB is ready to be tested on
        heavy performance cases.

        Time(insert the dataset) + Time(optimize) will be recorded as "load_duration" metric
        Optimize's execution time is limited, the limited time is based on cases.
        """

    @property
    def application_package(self):
        if getattr(self, "_application_package", None) is None:
            self._application_package = self._create_application_package()
        return self._application_package

    def _create_application_package(self):
        from vespa.package import (
            HNSW,
            ApplicationPackage,
            Document,
            Field,
            QueryField,
            QueryProfile,
            RankProfile,
            Schema,
            Validation,
            ValidationID,
        )

        # binary keeps a float source field; the binarize/pack_bits pipeline below
        # derives the packed int8 field from it.
        tensor_type = {
            "none": "float",
            "binary": "float",
            "bfloat16": "bfloat16",
            "int8": "int8",
        }[self.case_config.quantization_type]

        fields = [
            Field(
                "id",
                "int",
                indexing=["summary", "attribute"],
            ),
            Field(
                "embedding",
                f"tensor<{tensor_type}>(x[{self.dim}])",
                indexing=["summary", "attribute", "index"],
                ann=HNSW(**self.case_config.index_param()),
            ),
        ]

        if self.case_config.quantization_type == "binary":
            fields.append(
                Field(
                    "embedding_binary",
                    f"tensor<int8>(x[{math.ceil(self.dim / 8)}])",
                    indexing=[
                        "input embedding",
                        # convert 32 bit float to 1 bit
                        "binarize",
                        # pack 8 bits into one int8
                        "pack_bits",
                        "summary",
                        "attribute",
                        "index",
                    ],
                    ann=HNSW(**{**self.case_config.index_param(), "distance_metric": "hamming"}),
                    is_document_field=False,
                )
            )

        tomorrow = datetime.date.today() + datetime.timedelta(days=1)

        return ApplicationPackage(
            "vectordbbench",
            [
                Schema(
                    self.schema_name,
                    Document(
                        fields,
                    ),
                    rank_profiles=[
                        RankProfile(
                            name="none",
                            first_phase="",
                            inherits="default",
                            inputs=[("query(query_embedding)", f"tensor<float>(x[{self.dim}])")],
                        ),
                        RankProfile(
                            name="binary",
                            first_phase="",
                            inherits="default",
                            inputs=[("query(query_embedding)", f"tensor<int8>(x[{math.ceil(self.dim / 8)}])")],
                        ),
                        RankProfile(
                            name="bfloat16",
                            first_phase="",
                            inherits="default",
                            inputs=[("query(query_embedding)", f"tensor<bfloat16>(x[{self.dim}])")],
                        ),
                        RankProfile(
                            name="int8",
                            first_phase="",
                            inherits="default",
                            inputs=[("query(query_embedding)", f"tensor<int8>(x[{self.dim}])")],
                        ),
                    ],
                )
            ],
            # Vespa caps `hits` at 400 by default, below the k used by some cases.
            query_profile=QueryProfile(fields=[QueryField(name="maxHits", value=10000)]),
            validations=[
                Validation(ValidationID.tensorTypeChange, until=tomorrow),
                Validation(ValidationID.fieldTypeChange, until=tomorrow),
            ],
        )

    _CONTENT_CLUSTER = "vectordbbench_content"
    _APP_PATH = "/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default/content/"

    def _existing_benchmark_schemas(self) -> dict[str, str]:
        """Schemas of OTHER benchmark collections that are already deployed (name -> .sd text).

        A Vespa deployment replaces the whole application, so a new collection would otherwise drop the existing
        ones. Only document types of our own content cluster are carried over; any other application is left alone.
        Any problem here is non-fatal: we fall back to deploying just our own schema (the previous behaviour).
        """
        import requests

        base = f'{self.db_config["url"]}:{self.db_config.get("config_port", 19071)}' + self._APP_PATH
        found: dict[str, str] = {}
        try:
            r = requests.get(base + "services.xml", timeout=10)
            if r.status_code != 200:
                return found
            for content in ET.fromstring(r.text).iter("content"):
                if content.get("id") != self._CONTENT_CLUSTER:
                    continue
                for doc in content.iter("document"):
                    name = doc.get("type")
                    if not name or name == self.schema_name:
                        continue
                    sd = requests.get(f"{base}schemas/{name}.sd", timeout=10)
                    if sd.status_code == 200:
                        found[name] = sd.text
        except Exception:
            log.warning("Could not read the deployed Vespa application; deploying only schema %s", self.schema_name, exc_info=True)
            return {}
        return found

    def _package_with_existing_schemas(self) -> io.BytesIO:
        """Our application package plus the schemas of other benchmark collections already on the server."""
        package = self.application_package.to_zip()
        others = self._existing_benchmark_schemas()
        if not others:
            return package
        log.info(f"Keeping existing Vespa schemas in the new deployment: {sorted(others)}")
        out = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(package.getvalue())) as zin, zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == "services.xml":
                    extra = "".join(f'            <document type="{n}" mode="index"></document>\n' for n in others)
                    data = data.decode().replace("        </documents>", extra + "        </documents>", 1).encode()
                zout.writestr(item, data)
            for name, sd in others.items():
                zout.writestr(f"schemas/{name}.sd", sd)
        out.seek(0)
        return out

    def _wait_until_deployed(self, timeout_s: int = 600) -> None:
        """Block until the new deployment is live and this schema can be fed and queried.

        prepareandactivate returns as soon as the config is activated, but the content and container nodes pick it
        up asynchronously. On a server that is already running, feeding or querying a NEW schema in that window
        fails ("Could not resolve source ref"); pyvespa's feed reports such failures only to a callback, so documents
        would be lost silently. Wait for (1) all services to converge on the new config generation and (2) the
        schema to answer a query.
        """
        import time

        import requests

        conf = f'{self.db_config["url"]}:{self.db_config.get("config_port", 19071)}' + self._APP_PATH.replace("content/", "serviceconverge")
        query = f'{self.db_config["url"]}:{self.db_config["port"]}/search/'
        deadline = time.time() + timeout_s
        converged = queryable = False
        while time.time() < deadline:
            try:
                if not converged:
                    converged = bool(requests.get(conf, timeout=10).json().get("converged"))
                if converged:
                    r = requests.get(query, params={"yql": f"select id from {self.schema_name} where true", "hits": 0}, timeout=10)
                    queryable = r.status_code == 200 and "errors" not in r.json().get("root", {})
                    if queryable:
                        log.info(f"Vespa deployment converged and schema {self.schema_name} is queryable")
                        return
            except Exception as e:  # services restarting
                log.debug(f"waiting for Vespa deployment: {e}")
            time.sleep(2)
        msg = f"Vespa deployment did not become ready within {timeout_s}s (converged={converged}, queryable={queryable})"
        raise RuntimeError(msg)

    def deploy_http(self) -> application.Vespa:
        """
        Deploy a Vespa application package via HTTP REST API.

        Returns:
            application.Vespa: The deployed Vespa application instance
        """
        import requests

        url = f'{self.db_config["url"]}:{self.db_config.get("config_port", 19071)}/application/v2/tenant/default/prepareandactivate'
        package_data = self._package_with_existing_schemas()
        headers = {"Content-Type": "application/zip"}

        try:
            response = requests.post(url=url, data=package_data, headers=headers, timeout=10)

            response.raise_for_status()
            result = response.json()
            self._wait_until_deployed()
            return application.Vespa(
                url=self.db_config["url"],
                port=self.db_config["port"],
                deployment_message=result.get("message"),
                application_package=self.application_package,
            )

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to deploy Vespa application: {e!s}"
            if hasattr(e, "response") and e.response is not None:
                error_msg += f" - Response: {e.response.text}"
            raise RuntimeError(error_msg) from e
