#!/usr/bin/python
"""iati scraper"""

import logging
from io import StringIO
from urllib.parse import urlencode

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
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

    def get_date_range(
        self, df_activities: pd.DataFrame, df_locations: pd.DataFrame
    ) -> dict:
        dates = []
        for df in (df_activities, df_locations):
            if "day_start" in df.columns and not df["day_start"].isna().all():
                day_starts = pd.to_datetime(df["day_start"], errors="coerce")
                dates.append(day_starts.min())
                dates.append(day_starts.max())

        if not dates:
            return {"min_date": None, "max_date": None}

        # Format dates
        min_dt = min(d for d in dates if pd.notnull(d))
        max_dt = max(d for d in dates if pd.notnull(d))
        return {
            "min_date": min_dt.strftime("%Y-%m-%d"),
            "max_date": max_dt.strftime("%Y-%m-%d"),
        }

    def fetch_df(self, template: str, iso2: str, prefix: str) -> pd.DataFrame:
        """
        Query d-portal to get country data
        """
        sql = template.format(iso2=iso2, threshold=self.DAY_END_THRESHOLD)
        params = {"form": "csv", "human": "1", "sql": sql}
        url = f"{self._configuration['base_url']}?{urlencode(params)}"
        filename = f"{prefix}-{iso2.lower()}.csv"
        raw_csv = self._retriever.download_text(url, filename)

        try:
            df = pd.read_csv(StringIO(raw_csv))
        except pd.errors.EmptyDataError:
            logger.warning("No %s data for %s", prefix, iso2)
            return pd.DataFrame()

        return df.fillna("")

    def get_activities_data(self, iso2: str) -> pd.DataFrame:
        return self.fetch_df(self.SQL_ACTIVITIES, iso2, "iati-activities")

    def get_locations_data(self, iso2: str) -> pd.DataFrame:
        return self.fetch_df(self.SQL_LOCATIONS, iso2, "iati-locations")

    def generate_dataset(self, country) -> Dataset:
        country_name = country["name"]
        iso2 = country["iso2"]
        iso3 = country["iso3"]

        # Get data
        df_activities = self.get_activities_data(iso2)
        df_locations = self.get_locations_data(iso2)

        # Make sure data is not empty
        if df_activities.empty and df_locations.empty:
            logger.warning("Skipping %s: both activities and locations are empty", iso2)
            return None

        date_range = self.get_date_range(df_activities, df_locations)

        # Create dataset
        title = self._configuration["title"].replace("(country)", country_name)
        slugified_name = f"iati-{iso3.lower()}"
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.add_country_location(country_name)
        dataset.add_tags(self._configuration["tags"])

        dataset.set_time_period(date_range["min_date"], date_range["max_date"])

        # Generate activities resource
        if not df_activities.empty:
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
        if not df_locations.empty:
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

        return dataset
