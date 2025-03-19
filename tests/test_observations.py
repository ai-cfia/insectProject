import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
from pydantic import ValidationError

from src.observations import (
    Observation,
    ObservationSummary,
    Region,
    get_all_observations,
    get_observations,
    transform_summaries_to_dataframe,
)
from src.settings import Settings


class TestGetObservations(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = Settings(api_request_delay=0.0)
        self.taxon_ids = None
        self.taxon_names = ["Panthera leo"]
        self.date_from = "2024-01-01"
        self.date_to = "2024-03-01"
        self.region = Region.CA
        self.per_page = 1
        self.page = 1

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_success(self, mock_observations_get):
        mock_observations_get.return_value = AsyncMock(
            total_results=468,
            results=[{"id": 12345, "created_at": datetime(2024, 2, 29, 17, 49, 36)}],
        )

        observations = await get_observations(
            settings=self.settings,
            taxon_ids=self.taxon_ids,
            taxon_names=self.taxon_names,
            date_from=self.date_from,
            date_to=self.date_to,
            region=self.region,
            per_page=self.per_page,
            page=self.page,
        )

        self.assertEqual(observations.total_results, 468)
        self.assertEqual(len(observations.results), 1)
        self.assertEqual(observations.results[0]["id"], 12345)

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_empty(self, mock_observations_get):
        mock_observations_get.return_value = AsyncMock(
            total_results=0,
            results=[],
        )

        observations = await get_observations(
            settings=self.settings,
            taxon_ids=self.taxon_ids,
            taxon_names=self.taxon_names,
            date_from=self.date_from,
            date_to=self.date_to,
            region=self.region,
            per_page=self.per_page,
            page=self.page,
        )

        self.assertEqual(observations.total_results, 0)
        self.assertEqual(len(observations.results), 0)

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_rate_limit(self, mock_observations_get):
        mock_observations_get.side_effect = Exception("429 Too Many Requests")

        with self.assertRaises(Exception) as context:
            await get_observations(
                settings=self.settings,
                taxon_ids=self.taxon_ids,
                taxon_names=self.taxon_names,
                date_from=self.date_from,
                date_to=self.date_to,
                region=self.region,
                per_page=self.per_page,
                page=self.page,
            )

        self.assertIn("429 Too Many Requests", str(context.exception))

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_coordinates(self, mock_observations_get):
        mock_observations_get.return_value = AsyncMock(
            total_results=1,
            results=[{"id": 12345, "created_at": datetime(2024, 2, 29, 17, 49, 36)}],
        )

        nelat, nelng, swlat, swlng = self.settings.areas["CA"]

        await get_observations(
            settings=self.settings,
            taxon_ids=self.taxon_ids,
            taxon_names=self.taxon_names,
            date_from=self.date_from,
            date_to=self.date_to,
            region=self.region,
            per_page=self.per_page,
            page=self.page,
        )

        mock_observations_get.assert_called_with(
            page=str(self.page),
            per_page=str(self.per_page),
            created_d1=datetime.strptime(self.date_from, "%Y-%m-%d"),
            created_d2=datetime.strptime(self.date_to, "%Y-%m-%d"),
            created_on=None,
            taxon_id=None,
            taxon_name=self.taxon_names,
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng,
        )

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_no_region(self, mock_observations_get):
        self.region = None  # Ensure no region is passed

        mock_observations_get.return_value = AsyncMock(
            total_results=1,
            results=[{"id": 12345, "created_at": datetime(2024, 2, 29, 17, 49, 36)}],
        )

        await get_observations(
            settings=self.settings,
            taxon_ids=self.taxon_ids,
            taxon_names=self.taxon_names,
            date_from=self.date_from,
            date_to=self.date_to,
            region=self.region,
            per_page=self.per_page,
            page=self.page,
        )

        mock_observations_get.assert_called_with(
            page=str(self.page),
            per_page=str(self.per_page),
            created_d1=datetime.strptime(self.date_from, "%Y-%m-%d"),
            created_d2=datetime.strptime(self.date_to, "%Y-%m-%d"),
            created_on=None,
            taxon_id=None,
            taxon_name=self.taxon_names,
            nelat=None,
            nelng=None,
            swlat=None,
            swlng=None,
        )

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_with_date_on(self, mock_observations_get):
        self.date_on = "2024-02-15"

        mock_observations_get.return_value = AsyncMock(
            total_results=1,
            results=[{"id": 54321, "created_at": datetime(2024, 2, 15, 12, 30, 0)}],
        )

        observations = await get_observations(
            settings=self.settings,
            taxon_ids=self.taxon_ids,
            taxon_names=self.taxon_names,
            date_on=self.date_on,
            region=self.region,
            per_page=self.per_page,
            page=self.page,
        )

        self.assertEqual(observations.total_results, 1)
        self.assertEqual(observations.results[0]["id"], 54321)

        nelat, nelng, swlat, swlng = self.settings.areas["CA"]

        mock_observations_get.assert_called_with(
            page=str(self.page),
            per_page=str(self.per_page),
            created_d1=None,
            created_d2=None,
            created_on=datetime.strptime(self.date_on, "%Y-%m-%d").date(),
            taxon_id=None,
            taxon_name=self.taxon_names,
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng,
        )

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_with_taxon_ids(self, mock_observations_get):
        self.taxon_ids = [123, 456, 789]

        mock_observations_get.return_value = AsyncMock(
            total_results=3,
            results=[{"id": 1}, {"id": 2}, {"id": 3}],
        )

        await get_observations(
            settings=self.settings,
            taxon_ids=self.taxon_ids,
            taxon_names=self.taxon_names,
            date_from=self.date_from,
            date_to=self.date_to,
            region=self.region,
            per_page=self.per_page,
            page=self.page,
        )

        mock_observations_get.assert_called_with(
            page=str(self.page),
            per_page=str(self.per_page),
            created_d1=datetime.strptime(self.date_from, "%Y-%m-%d"),
            created_d2=datetime.strptime(self.date_to, "%Y-%m-%d"),
            created_on=None,
            taxon_id=["123", "456", "789"],
            taxon_name=self.taxon_names,
            nelat=self.settings.areas["CA"][0],
            nelng=self.settings.areas["CA"][1],
            swlat=self.settings.areas["CA"][2],
            swlng=self.settings.areas["CA"][3],
        )

    @patch(
        "inaturalist_client.ObservationsApi.observations_get", new_callable=AsyncMock
    )
    async def test_get_observations_with_defaults(self, mock_observations_get):
        self.taxon_ids = None
        self.taxon_names = None
        self.date_from = None
        self.date_to = None
        self.date_on = None
        self.region = None

        mock_observations_get.return_value = AsyncMock(
            total_results=0,
            results=[],
        )

        await get_observations(
            self.settings,
            self.taxon_ids,
            self.taxon_names,
            self.date_from,
            self.date_to,
            self.date_on,
            self.region,
            self.per_page,
            self.page,
        )

        mock_observations_get.assert_called_with(
            page=str(self.page),
            per_page=str(self.per_page),
            created_d1=None,
            created_d2=None,
            created_on=None,
            taxon_id=None,
            taxon_name=None,
            nelat=None,
            nelng=None,
            swlat=None,
            swlng=None,
        )


class TestObservationSummary(unittest.TestCase):
    def setUp(self):
        self.example_data = {
            "id": 200760772,
            "quality_grade": "casual",
            "taxon": {
                "preferred_common_name": "Lion",
                "name": "Panthera leo",
                "iconic_taxon_name": "Mammalia",
                "iconic_taxon_id": 40151,
            },
            "photos": [
                {"url": "https://static.inaturalist.org/photos/354262857/square.jpg"}
            ],
            "geojson": {"coordinates": [-87.7391541636, 41.984022249]},
            "created_at_details": {"var_date": date(2024, 2, 28)},
            "observed_on": datetime(2024, 2, 28, 0, 0),
            "user": {"login": "spencer_palmer"},
            "uri": "https://www.inaturalist.org/observations/200760772",
        }

    def test_valid_initialization(self):
        obs = ObservationSummary.model_validate(self.example_data)
        self.assertEqual(obs.id, 200760772)
        self.assertEqual(obs.quality_grade, "casual")
        self.assertEqual(obs.name, "Lion")
        self.assertEqual(obs.name_alt, "Panthera leo")
        self.assertEqual(obs.uri, "https://www.inaturalist.org/observations/200760772")
        self.assertEqual(
            obs.image_urls,
            ["https://static.inaturalist.org/photos/354262857/large.jpg"],
        )
        self.assertEqual(obs.coordinates, [-87.7391541636, 41.984022249])
        self.assertEqual(obs.created_at, datetime(2024, 2, 28, 0, 0))
        self.assertEqual(obs.observed_at, datetime(2024, 2, 28, 0, 0))
        self.assertEqual(obs.username, "spencer_palmer")
        self.assertEqual(obs.taxon_name, "Mammalia")
        self.assertEqual(obs.taxon_id, 40151)

    def test_missing_fields(self):
        data = self.example_data.copy()
        del data["id"]
        with self.assertRaises(ValidationError):
            ObservationSummary.model_validate(data)

    def test_empty_photos(self):
        data = self.example_data.copy()
        data["photos"] = []
        obs = ObservationSummary.model_validate(data)
        self.assertEqual(obs.image_urls, [])

    def test_invalid_coordinates(self):
        data = self.example_data.copy()
        data["geojson"]["coordinates"] = "invalid"
        with self.assertRaises(ValidationError):
            ObservationSummary.model_validate(data)

    def test_model_validate_from_dict(self):
        obs = ObservationSummary.model_validate(self.example_data)
        obs_dict = obs.model_dump()
        obs_from_dict = ObservationSummary.model_validate(obs_dict)
        self.assertEqual(obs, obs_from_dict)

    def test_missing_preferred_common_name(self):
        data = self.example_data.copy()
        del data["taxon"]["preferred_common_name"]
        obs = ObservationSummary.model_validate(data)
        self.assertEqual(obs.name, "")

    def test_none_preferred_common_name(self):
        data = self.example_data.copy()
        data["taxon"]["preferred_common_name"] = None
        obs = ObservationSummary.model_validate(data)
        self.assertIsNone(obs.name)


class TestGetAllObservations(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = Settings(api_request_delay=0.0)
        self.taxon_ids = [47115]
        self.per_page = 2
        self.page = 1

    @patch("src.observations.get_observations", new_callable=AsyncMock)
    async def test_get_all_observations_multiple_pages(self, mock_get_observations):
        mock_get_observations.side_effect = [
            AsyncMock(results=[Observation(id=1), Observation(id=2)]),
            AsyncMock(results=[Observation(id=3), Observation(id=4)]),
            AsyncMock(results=[]),
        ]

        observations = await get_all_observations(
            settings=self.settings, taxon_ids=self.taxon_ids, per_page=self.per_page
        )

        self.assertEqual(len(observations), 4)
        self.assertEqual([obs.id for obs in observations], [1, 2, 3, 4])

    @patch("src.observations.get_observations", new_callable=AsyncMock)
    async def test_get_all_observations_single_page(self, mock_get_observations):
        mock_get_observations.return_value = AsyncMock(results=[Observation(id=1)])

        observations = await get_all_observations(
            settings=self.settings, taxon_ids=self.taxon_ids, per_page=self.per_page
        )

        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].id, 1)

    @patch("src.observations.get_observations", new_callable=AsyncMock)
    async def test_get_all_observations_empty_response(self, mock_get_observations):
        mock_get_observations.return_value = AsyncMock(results=[])

        observations = await get_all_observations(
            settings=self.settings, taxon_ids=self.taxon_ids, per_page=self.per_page
        )

        self.assertEqual(len(observations), 0)

    @patch("src.observations.get_observations", new_callable=AsyncMock)
    async def test_get_all_observations_pagination_logic(self, mock_get_observations):
        mock_get_observations.side_effect = [
            AsyncMock(results=[Observation(id=1), Observation(id=2)]),
            AsyncMock(results=[Observation(id=3)]),
            AsyncMock(results=[]),
        ]

        observations = await get_all_observations(
            settings=self.settings, taxon_ids=self.taxon_ids, per_page=self.per_page
        )

        self.assertEqual(len(observations), 3)
        self.assertEqual([obs.id for obs in observations], [1, 2, 3])

    @patch("src.observations.get_observations", new_callable=AsyncMock)
    async def test_get_all_observations_returned_structure(self, mock_get_observations):
        mock_get_observations.return_value = AsyncMock(
            results=[Observation(id=1, quality_grade="research")]
        )

        observations = await get_all_observations(
            settings=self.settings, taxon_ids=self.taxon_ids, per_page=self.per_page
        )

        self.assertEqual(len(observations), 1)
        self.assertIsInstance(observations[0], Observation)
        self.assertEqual(observations[0].id, 1)
        self.assertEqual(observations[0].quality_grade, "research")


class TestTransformSummariesToDataFrame(unittest.TestCase):
    def setUp(self):
        self.sample_summaries = [
            ObservationSummary(
                id=1,
                quality_grade="research",
                name="Lion",
                name_alt="Panthera leo",
                uri="https://example.com/1",
                image_urls=["https://example.com/image1.jpg"],
                coordinates=[-87.739154, 41.984022],
                created_at=datetime(2024, 3, 10, 12, 0, 0),
                observed_at=datetime(2024, 3, 9, 15, 30, 0),
                username="user1",
                taxon_name="Mammalia",
                taxon_id=40151,
            ),
            ObservationSummary(
                id=2,
                quality_grade="casual",
                name="Tiger",
                name_alt="Panthera tigris",
                uri="https://example.com/2",
                image_urls=["https://example.com/image2.jpg"],
                coordinates=[-85.123456, 42.123456],
                created_at=datetime(2024, 3, 11, 14, 0, 0),
                observed_at=datetime(2024, 3, 10, 16, 45, 0),
                username="user2",
                taxon_name="Mammalia",
                taxon_id=40152,
            ),
        ]
        self.columns = [
            "Obsvn_ID",
            "Quality",
            "Name1",
            "Name2",
            "Obsvn_URL",
            "Sample_Img",
            "coordinates",
            "posted_on",
            "observed_on",
            "user_login",
            "upper taxa",
            "upper_taxa_id",
        ]

    def test_transform_valid_input(self):
        df = transform_summaries_to_dataframe(self.sample_summaries, self.columns)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(len(df.columns), len(self.columns))

    def test_column_mapping(self):
        df = transform_summaries_to_dataframe(self.sample_summaries, self.columns)
        self.assertListEqual(list(df.columns), self.columns)

    def test_missing_fields(self):
        partial_summaries = [ObservationSummary(id=3, quality_grade="research")]
        df = transform_summaries_to_dataframe(partial_summaries, self.columns)
        self.assertEqual(len(df), 1)
        self.assertTrue(all(col in df.columns for col in self.columns))

    def test_output_format_consistency(self):
        df1 = transform_summaries_to_dataframe(self.sample_summaries, self.columns)
        df2 = transform_summaries_to_dataframe(self.sample_summaries, self.columns)
        pd.testing.assert_frame_equal(df1, df2)
