import unittest
from unittest.mock import MagicMock, patch

from src.geo import get_city_province_country
from tests import settings


class TestGeocoding(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()

    @patch("geopy.geocoders.Nominatim.reverse")
    def test_city_fallbacks(self, mock_reverse):
        test_cases = [
            ({"city": "New York"}, "New York"),
            ({"town": "Brooklyn"}, "Brooklyn"),
            ({"village": "Greenwich"}, "Greenwich"),
            ({"municipality": "Manhattan"}, "Manhattan"),
            ({"county": "Los Angeles County"}, "Los Angeles County"),
            ({"state_district": "Greater London"}, "Greater London"),
            ({}, ""),  # No matching field
        ]

        for address, expected_city in test_cases:
            mock_reverse.return_value = MagicMock(raw={"address": address})
            city, _, _ = get_city_province_country(self.settings, 0, 0)
            self.assertEqual(city, expected_city)

    @patch("geopy.geocoders.Nominatim.reverse")
    def test_valid_coordinates(self, mock_reverse):
        mock_reverse.side_effect = [
            MagicMock(
                raw={
                    "address": {
                        "city": "New York",
                        "state": "New York",
                        "country_code": "us",
                    }
                }
            ),
            MagicMock(
                raw={
                    "address": {
                        "city": "London",
                        "state": "England",
                        "country_code": "gb",
                    }
                }
            ),
            MagicMock(
                raw={
                    "address": {"city": "Tokyo", "state": "Tokyo", "country_code": "jp"}
                }
            ),
            MagicMock(
                raw={
                    "address": {
                        "city": "Sydney",
                        "state": "New South Wales",
                        "country_code": "au",
                    }
                }
            ),
            MagicMock(
                raw={
                    "address": {
                        "city": "Paris",
                        "state": "Île-de-France",
                        "country_code": "fr",
                    }
                }
            ),
        ]

        test_cases = [
            ((40.7128, -74.0060), ("New York", "New York", "us")),
            ((51.5074, -0.1278), ("London", "England", "gb")),
            ((35.6895, 139.6917), ("Tokyo", "Tokyo", "jp")),
            ((-33.8688, 151.2093), ("Sydney", "New South Wales", "au")),
            ((48.8566, 2.3522), ("Paris", "Île-de-France", "fr")),
        ]

        for i, (coord, expected) in enumerate(test_cases):
            city, province, country = get_city_province_country(
                self.settings, coord[0], coord[1]
            )
            self.assertEqual((city, province, country), expected)

    @patch("geopy.geocoders.Nominatim.reverse")
    def test_no_city_key(self, mock_reverse):
        mock_reverse.return_value = MagicMock(
            raw={"address": {"state": "California", "country_code": "us"}}
        )
        city, province, country = get_city_province_country(
            self.settings, 36.7783, -119.4179
        )
        self.assertEqual((city, province, country), ("", "California", "us"))

    @patch("geopy.geocoders.Nominatim.reverse")
    def test_empty_address(self, mock_reverse):
        mock_reverse.return_value = MagicMock(raw={"address": {}})
        city, province, country = get_city_province_country(self.settings, 0.0, 0.0)
        self.assertEqual((city, province, country), ("", "", ""))

    @patch("geopy.geocoders.Nominatim.reverse", side_effect=Exception("API Error"))
    def test_exception_handling(self, mock_reverse):
        city, province, country = get_city_province_country(
            self.settings, 55.7558, 37.6173
        )
        self.assertEqual((city, province, country), ("", "", ""))
