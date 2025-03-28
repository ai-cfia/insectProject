import unittest
from unittest.mock import AsyncMock, patch

from src.species import get_specie_ids
from tests import settings


class TestGetSpecieIds(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.settings.project_id = "1"

    @patch("src.species.ApiClient")
    @patch("src.species.ProjectsApi")
    async def test_returns_taxon_ids(self, mock_api_class, mock_api_client):
        mock_api = AsyncMock()
        mock_api.projects_id_get.return_value.results = [
            AsyncMock(
                project_observation_rules=[
                    AsyncMock(taxon=AsyncMock(id=1)),
                    AsyncMock(taxon=AsyncMock(id=2)),
                    AsyncMock(taxon=None),
                ]
            )
        ]
        mock_api_class.return_value = mock_api

        ids = await get_specie_ids(self.settings)
        self.assertEqual(ids, [1, 2])

    @patch("src.species.ApiClient")
    @patch("src.species.ProjectsApi")
    async def test_returns_empty_list_on_empty_results(
        self, mock_api_class, mock_api_client
    ):
        mock_api = AsyncMock()
        mock_api.projects_id_get.return_value.results = []
        mock_api_class.return_value = mock_api

        ids = await get_specie_ids(self.settings)
        self.assertEqual(ids, [])

    @patch("src.species.ApiClient")
    @patch("src.species.ProjectsApi")
    async def test_skips_none_taxon(self, mock_api_class, mock_api_client):
        mock_api = AsyncMock()
        mock_api.projects_id_get.return_value.results = [
            AsyncMock(
                project_observation_rules=[
                    AsyncMock(taxon=None),
                    AsyncMock(taxon=AsyncMock(id=4)),
                ]
            )
        ]
        mock_api_class.return_value = mock_api

        ids = await get_specie_ids(self.settings)
        self.assertEqual(ids, [4])

    @patch("src.species.ApiClient")
    @patch("src.species.ProjectsApi")
    async def test_skips_taxon_with_none_id(self, mock_api_class, mock_api_client):
        mock_api = AsyncMock()
        mock_api.projects_id_get.return_value.results = [
            AsyncMock(
                project_observation_rules=[
                    AsyncMock(taxon=AsyncMock(id=None)),
                    AsyncMock(taxon=AsyncMock(id=5)),
                ]
            )
        ]
        mock_api_class.return_value = mock_api

        ids = await get_specie_ids(self.settings)
        self.assertEqual(ids, [5])
