import copy
import unittest
from unittest.mock import patch

import pandas as pd

from src.preprocess import (
    filter_ca_us_locations,
    group_by_taxa,
    keep_only_first_sample_image,
)
from src.observation_reports import exclude_non_invasive
from tests import settings


class TestFilterNorthAmericanLocations(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.settings.coords_column = "coordinates"
        self.settings.city_column = "City"
        self.settings.province_column = "Province"

    @patch("src.preprocess.get_city_province_country")
    def test_filter_north_american_locations(self, mock_get_city_province_country):
        mock_get_city_province_country.side_effect = [
            ("Ottawa", "ON", "ca"),
            ("New York", "NY", "us"),
            ("Paris", "Île-de-France", "fr"),
        ]

        test_data = {
            "coordinates": [
                (45.4215, -75.6993),
                (40.7128, -74.0060),
                (48.8566, 2.3522),
            ]
        }

        df = pd.DataFrame(test_data)
        filtered_df = filter_ca_us_locations(self.settings, df)

        expected_data = {
            "City": ["Ottawa", "New York"],
            "Province": ["ON", "NY"],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(filtered_df, expected_df)

    @patch("src.preprocess.get_city_province_country")
    def test_filter_all_non_north_american_locations(
        self, mock_get_city_province_country
    ):
        mock_get_city_province_country.side_effect = [
            ("Paris", "Île-de-France", "fr"),
            ("Tokyo", "Tokyo", "jp"),
        ]

        test_data = {
            "coordinates": [
                (48.8566, 2.3522),
                (35.6895, 139.6917),
            ]
        }

        df = pd.DataFrame(test_data)
        filtered_df = filter_ca_us_locations(self.settings, df)

        self.assertTrue(filtered_df.empty)

    @patch("src.preprocess.get_city_province_country")
    def test_filter_empty_dataframe(self, mock_get_city_province_country):
        df = pd.DataFrame({"coordinates": []})
        filtered_df = filter_ca_us_locations(self.settings, df)
        self.assertTrue(filtered_df.empty)


class TestExtractFirstSampleImage(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.settings.image_column = "Sample_Img"
        self.test_data = pd.DataFrame(
            {
                "Sample_Img": [
                    ["image1.jpg", "image2.jpg"],
                    ["image3.jpg"],
                    [],
                    None,
                    ["image4.jpg", "image5.jpg"],
                ]
            }
        )

    def test_extract_first_sample_image(self):
        expected_output = pd.DataFrame(
            {
                "Sample_Img": [
                    "image1.jpg",
                    "image3.jpg",
                    None,
                    None,
                    "image4.jpg",
                ]
            }
        )

        result = keep_only_first_sample_image(self.settings, self.test_data.copy())

        pd.testing.assert_frame_equal(result, expected_output)

    def test_missing_sample_img_column(self):
        df = pd.DataFrame({"Other_Column": [1, 2, 3]})
        self.settings.image_column = "Sample_Img"
        result = keep_only_first_sample_image(self.settings, df.copy())
        pd.testing.assert_frame_equal(result, df)


class TestSplitByTaxonomy(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.settings.upper_taxa_column = "upper taxa"
        self.settings.upper_taxa = ["Insecta", "Mollusca", "Plantae", "Fungi"]

        self.test_data = {
            "Species": [
                "Species1",
                "Species2",
                "Species3",
                "Species4",
                "Species5",
                "Species6",
            ],
            "upper taxa": [
                "Insecta",
                "Mollusca",
                "Plantae",
                "Fungi",
                "Unknown",
                "Insecta",
            ],
            "Value": [10, 20, 30, 40, 50, 60],
        }
        self.df = pd.DataFrame(self.test_data)

    def test_correct_number_of_outputs(self):
        results = group_by_taxa(self.settings, self.df)
        self.assertEqual(len(results), len(self.settings.upper_taxa) + 1)

    def test_individual_taxa_dataframes(self):
        insecta_df, mollusca_df, plantae_df, fungi_df, others_df = group_by_taxa(
            self.settings, self.df
        )

        for taxon, df_out in zip(
            self.settings.upper_taxa, [insecta_df, mollusca_df, plantae_df, fungi_df]
        ):
            expected = (
                self.df[self.df[self.settings.upper_taxa_column] == taxon]
                .drop(columns=[self.settings.upper_taxa_column])
                .reset_index(drop=True)
            )
            pd.testing.assert_frame_equal(df_out, expected)

    def test_others_dataframe(self):
        *_, others_df = group_by_taxa(self.settings, self.df)

        expected_others = (
            self.df[
                ~self.df[self.settings.upper_taxa_column].isin(self.settings.upper_taxa)
            ]
            .drop(columns=[self.settings.upper_taxa_column])
            .reset_index(drop=True)
        )

        if expected_others.empty:
            self.assertIsNone(others_df)
        else:
            pd.testing.assert_frame_equal(others_df, expected_others)

    def test_empty_dataframe(self):
        empty_df = pd.DataFrame(columns=["Species", "upper taxa", "Value"])
        results = group_by_taxa(self.settings, empty_df)
        for df in results:
            self.assertTrue(df is None or df.empty)

    def test_missing_taxa_column(self):
        df_no_taxa = self.df.drop(columns=["upper taxa"])
        with self.assertRaises(KeyError):
            group_by_taxa(self.settings, df_no_taxa)


class TestExcludeNonInvasive(unittest.TestCase):
    def setUp(self):
        self.s = copy.deepcopy(settings.model_copy())
        self.s.name_alt_column = "Scientific Name"
        self.df = pd.DataFrame(
            {
                "Scientific Name": [
                    "Species A",
                    "Monochamus scutellatus",
                    "Species C",
                    "Species D",
                ]
            }
        )

    def test_exclude_matching_species(self):
        result = exclude_non_invasive(self.s, self.df)
        self.assertEqual(len(result), 3)
        self.assertNotIn(
            "Monochamus scutellatus", result[self.s.name_alt_column].values
        )

    def test_exclude_no_match(self):
        self.s.species_data.non_invasive = [self.s.species_data.invasive[0]]
        result = exclude_non_invasive(self.s, self.df)
        self.assertEqual(len(result), 4)

    def test_exclude_empty_list(self):
        self.s.species_data.non_invasive = []
        result = exclude_non_invasive(self.s, self.df)
        self.assertEqual(len(result), 4)
