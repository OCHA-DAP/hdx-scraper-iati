from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.iati.iati import IATI


class TestIATI:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "iati", "config")

    def test_iati(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestIATI",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )

                iati = IATI(configuration, retriever, tempdir)

                datasets = iati.generate_datasets()
                dataset = datasets[0]
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "caveats": "Information originates from multiple IATI reporting "
                    "organisations, and has not been centrally vetted or audited for "
                    "accuracy or consistency.\n"
                    "\n"
                    "Includes only those activities from the [IATI "
                    "Registry](https://iatiregistry.org/) that are included in "
                    "[D-Portal](http://www.d-portal.org/ctrack.html#view=search) and "
                    'have the status "Implementing".\n'
                    "\n"
                    "The total number of activities may include duplicates, if (for "
                    "example) a donor and an implementing partner both report the same "
                    "activity under different IATI identifiers.\n"
                    "\n"
                    "Start and end dates of activities within the dataset will "
                    "differ.\n",
                    "name": "current-iati-aid-activities-in-afghanistan",
                    "title": "Current IATI Aid Activities in Afghanistan",
                    "dataset_date": "[2019-01-01T00:00:00 TO 2020-01-01T23:59:59]",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "who is doing what and where-3w-4w-5w",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "hdx-other",
                    "license_other": "Allowed licenses for IATI reporting organisations are listed [here](https://iatistandard.org/en/guidance/preparing-organisation/organisation-data-publication/how-to-license-your-data/)",
                    "methodology": "Registry",
                    "dataset_source": "Various IATI reporting organisations",
                    "groups": [{"name": "afg"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "6b297b9d-ead6-458d-ae1b-1b9e9f61dd00",
                    "owner_org": "87f30a06-6085-473d-87d8-ab4c3aa36817",
                    "data_update_frequency": 30,
                    "notes": "List of active aid activities for (country) shared via the International Aid Transparency Initiative (IATI). Includes both humanitarian and development activities. More information on each activity (including financial data) is available from [http://www.d-portal.org](http://www.d-portal.org)",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "IATI activities in Afghanistan (no location information)",
                        "description": "Currently-active IATI activities in Afghanistan, in 3W/4W style with HXL hashtags. This dataset contains one unique activity/sector combination on each row. It is suitable for counting the total number of reported activities, or for aggregating activities by sector, reporting organisation, etc.",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "IATI activity locations in Afghanistan",
                        "description": "Current IATI activity locations in Afghanistan, in 3W/4W style with HXL hashtags. This dataset contains one row per location, so activities with multiple locations are repeated, and activities without location information are omitted. It is suitable for applications that want to show activity locations on a map, or find the closest geolocated activities to a settlement or camp.",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
