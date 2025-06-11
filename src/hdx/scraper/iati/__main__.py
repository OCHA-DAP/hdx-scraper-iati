#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.iati.iati import IATI

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-iati"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: iati"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    # User.check_current_user_write_access("<insert org name>")

    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            #
            # Steps to generate dataset
            #

            countriesdata = Country.countriesdata()
            countries = []
            for iso3_key, attrs in countriesdata["countries"].items():
                countries.append(
                    {
                        "iso2": attrs.get("#country+code+v_iso2"),
                        "iso3": attrs.get("#country+code+v_iso3"),
                        "name": attrs.get("#country+name+preferred"),
                    }
                )

            configuration = Configuration.read()
            iati = IATI(configuration, retriever, temp_dir)

            for country in countries:
                dataset = iati.generate_dataset(country)

                if dataset is None:
                    logger.warning(
                        "generate_dataset returned None for %s; skipping", country
                    )
                    continue

                dataset.update_from_yaml(
                    path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
