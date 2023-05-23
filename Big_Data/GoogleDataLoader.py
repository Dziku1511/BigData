import os as os
import pandas as pd
# noinspection PyUnresolvedReferences
from google.cloud import bigquery as bg
# noinspection PyUnresolvedReferences
from google.api_core.exceptions import BadRequest


class GoogleDataLoader:
    def __init__(self, limit: int = 1000):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./config/project-bigdata-0-afd72f9414bd.json"
        self.datatable: str = "bigquery-public-data.covid19_open_data.covid19_open_data"
        self.client: bg.Client = bg.Client()
        self.limit: int = limit
        self.fields: list[str] = ["*"]
        self.query: str = ""
        self.buildQuery()

    def buildQuery(self) -> None:
        if self.fields.__contains__("*"):
            fs: str = "*"
        else:
            fs: str = ', '.join(list(map(lambda x: f't.{x}', self.fields)))
        self.query: str = f"SELECT {fs} FROM `{self.datatable}` AS t LIMIT {self.limit}"

    def getData(self) -> pd.DataFrame:
        self.buildQuery()
        try:
            return self.client.query(self.query).result().to_dataframe()
        except BadRequest as e:
            print(f"Query error: {self.query}\n\t{e.errors}")

    def setQueryParams(self, fields: list[str] = ("*",), limit: int = -1) -> None:
        self.limit: int = limit if limit != -1 else self.limit
        self.fields: list[str] = fields
        self.buildQuery()

    def get(self, query: str, rest: str = "", limit: int = -1, source: str = "") -> pd.DataFrame:
        limit: int = limit if limit != -1 else self.limit
        query = query.replace("datatable", self.datatable)
        source = self.datatable if source == "" else source
        q: str = f"SELECT {query} FROM `{source}` AS t {rest} LIMIT {limit}"
        # print(q)
        return self.client\
            .query(q)\
            .result()\
            .to_dataframe()

