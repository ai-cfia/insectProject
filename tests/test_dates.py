import unittest
from datetime import datetime
from unittest.mock import patch

from src.dates import get_recent_dates


class TestGetRecentDates(unittest.TestCase):
    @patch("src.dates.get_yesterday", return_value=datetime(2024, 3, 26))
    def test_get_recent_dates(self, mock_get_yesterday):
        self.assertEqual(
            get_recent_dates(3),
            [datetime(2024, 3, 24), datetime(2024, 3, 25), datetime(2024, 3, 26)],
        )
