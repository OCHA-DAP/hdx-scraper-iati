#!/usr/bin/python
"""iati scraper"""

import logging
from io import StringIO
from typing import List
from urllib.parse import urlencode

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class IATI:
    DAY_END_THRESHOLD = 17500
    SQL_ACTIVITIES = (
        "SELECT * FROM act "
        "LEFT JOIN sector ON act.aid = sector.aid "
        "JOIN country ON act.aid = country.aid "
        "WHERE country.country_code = '{iso2}' "
        "AND day_end >= {threshold}"
    )
    SQL_LOCATIONS = (
        "SELECT * FROM act "
        "JOIN country ON act.aid = country.aid "
        "JOIN location ON act.aid = location.aid "
        "WHERE country.country_code = '{iso2}' "
        "AND day_end >= {threshold}"
    )

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def get_countries(self) -> list:
        """
        Gets list of country iso2 codes from local json file
        """
        data_json = self._retriever.download_json("countries.json")
        iso2_list = []
        for item in data_json.get("data", []):
            if "iso2" in item:
                iso2_list.append(item["iso2"])

        return iso2_list

    def fetch_df(self, template: str, iso2: str, prefix: str) -> pd.DataFrame:
        """
        Query d-portal to get country data
        """
        sql = template.format(iso2=iso2, threshold=self.DAY_END_THRESHOLD)
        params = {"form": "csv", "human": "1", "sql": sql}
        url = f"{self._configuration['base_url']}?{urlencode(params)}"
        filename = f"{prefix}-{iso2.lower()}.csv"
        raw_csv = self._retriever.download_text(url, filename)
        df = pd.read_csv(StringIO(raw_csv))
        return df.fillna("")

    def get_activities_data(self, iso2: str) -> pd.DataFrame:
        return self.fetch_df(self.SQL_ACTIVITIES, iso2, "iati-activities")

    def get_locations_data(self, iso2: str) -> pd.DataFrame:
        return self.fetch_df(self.SQL_LOCATIONS, iso2, "iati-locations")

    def generate_datasets(self) -> List[Dataset]:
        datasets = []
        countries = self.get_countries()

        for iso2 in countries:
            # Get data
            df_activities = self.get_activities_data(iso2)
            df_locations = self.get_locations_data(iso2)

            # min_date = df["day_start"].min()
            # max_date = df["day_start"].max()

            # Create dataset
            country_name = Country.get_country_name_from_iso2(iso2)
            title = self._configuration["title"].replace("(country)", country_name)
            slugified_name = slugify(title)
            logger.info(f"Creating dataset: {title}")
            dataset = Dataset(
                {
                    "name": slugified_name,
                    "title": title,
                }
            )
            dataset.add_country_location(country_name)
            dataset.add_tags(self._configuration["tags"])
            dataset.set_time_period("2019-01-01", "2020-01-01")

            # Generate activities resource
            resource_activities_name = self._configuration["title_activities"].replace(
                "(country)", country_name
            )
            slug_activities = slugify(resource_activities_name)
            resource_activities_data = {
                "name": resource_activities_name,
                "description": self._configuration["description_activities"].replace(
                    "(country)", country_name
                ),
                "format": "CSV",
            }
            dataset.generate_resource_from_iterable(
                headers=df_activities.columns.tolist(),
                iterable=df_activities.to_dict(orient="records"),
                hxltags=self._configuration["hxl_tags"],
                folder=self._temp_dir,
                filename=f"{slug_activities}.csv",
                resourcedata=resource_activities_data,
                quickcharts=None,
            )

            # Generate locations resource
            resource_locations_name = self._configuration["title_locations"].replace(
                "(country)", country_name
            )
            slug_locations = slugify(resource_locations_name)
            resource_locations_data = {
                "name": resource_locations_name,
                "description": self._configuration["description_locations"].replace(
                    "(country)", country_name
                ),
            }
            dataset.generate_resource_from_iterable(
                headers=df_locations.columns.tolist(),
                iterable=df_locations.to_dict(orient="records"),
                hxltags=self._configuration["hxl_tags"],
                folder=self._temp_dir,
                filename=f"{slug_locations}.csv",
                resourcedata=resource_locations_data,
                quickcharts=None,
            )

            datasets.append(dataset)

        return datasets
