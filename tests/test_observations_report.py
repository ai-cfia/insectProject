import unittest

import pandas as pd

from src.observation_reports import build_observations_email_tables
from src.settings import Settings


class TestBuildEmailTables(unittest.TestCase):
    def setUp(self):
        self.settings = Settings(
            smtp_host="smtp.test.com",
            smtp_port=587,
            smtp_username="test_user",
            smtp_password="test_pass",
            sender_email="sender@test.com",
            observations_email_recipients=["recipient@test.com"],
            comments_email_recipients=["recipient@test.com"],
            upper_taxa=["Insecta", "Plantae"],
            upper_taxa_column="upper taxa",
            name_column="Common Name",
            other_taxa_label="Other",
        )
        self.columns = ["upper taxa", "Common Name", "Observation URL"]

    def test_all_empty(self):
        df = pd.DataFrame(columns=self.columns)
        tables = build_observations_email_tables(self.settings, df, df)
        self.assertEqual(tables, [])

    def test_only_global(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Insecta"],
                "Common Name": ["A"],
                "Observation URL": ["http://a.com"],
            }
        )
        tables = build_observations_email_tables(
            self.settings, df, pd.DataFrame(columns=self.columns)
        )
        self.assertEqual(len(tables), 1)
        self.assertTrue(tables[0].title.startswith("iconic_taxa: Insecta"))
        self.assertIn("http://a.com", tables[0].html)

    def test_only_us(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Plantae"],
                "Common Name": ["B"],
                "Observation URL": ["http://b.com"],
            }
        )
        tables = build_observations_email_tables(
            self.settings, pd.DataFrame(columns=self.columns), df
        )
        self.assertEqual(len(tables), 1)
        self.assertTrue(tables[0].title.startswith("US iconic_taxa: Plantae"))
        self.assertIn("http://b.com", tables[0].html)

    def test_other_category(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Fungi"],
                "Common Name": ["X"],
                "Observation URL": ["http://x.com"],
            }
        )
        tables = build_observations_email_tables(self.settings, df, df)
        titles = [t.title for t in tables]
        self.assertIn("iconic_taxa: Other", titles)
        self.assertIn("US iconic_taxa: Other", titles)

    def test_multiple_categories(self):
        df = pd.DataFrame(
            {
                "upper taxa": ["Insecta", "Plantae", "Fungi"],
                "Common Name": ["A", "B", "C"],
                "Observation URL": ["http://a.com", "http://b.com", "http://c.com"],
            }
        )
        tables = build_observations_email_tables(self.settings, df, df)
        titles = [t.title for t in tables]
        expected = {
            "iconic_taxa: Insecta",
            "iconic_taxa: Plantae",
            "iconic_taxa: Other",
            "US iconic_taxa: Insecta",
            "US iconic_taxa: Plantae",
            "US iconic_taxa: Other",
        }
        self.assertEqual(set(titles), expected)
