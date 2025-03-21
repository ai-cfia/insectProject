import unittest
from unittest.mock import patch

import pandas as pd

from src.preprocess import (
    extract_first_sample_image,
    filter_north_american_locations,
    group_by_taxa,
)


class TestFilterNorthAmericanLocations(unittest.TestCase):
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
        filtered_df = filter_north_american_locations(df)

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
        filtered_df = filter_north_american_locations(df)

        self.assertTrue(filtered_df.empty)

    @patch("src.preprocess.get_city_province_country")
    def test_filter_empty_dataframe(self, mock_get_city_province_country):
        df = pd.DataFrame({"coordinates": []})
        filtered_df = filter_north_american_locations(df)
        self.assertTrue(filtered_df.empty)


class TestExtractFirstSampleImage(unittest.TestCase):
    def setUp(self):
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

        result = extract_first_sample_image(self.test_data.copy())

        pd.testing.assert_frame_equal(result, expected_output)

    def test_missing_sample_img_column(self):
        df = pd.DataFrame({"Other_Column": [1, 2, 3]})
        result = extract_first_sample_image(df.copy())
        pd.testing.assert_frame_equal(result, df)


class TestSplitByTaxonomy(unittest.TestCase):
    def setUp(self):
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
        self.upper_taxa = ["Insecta", "Mollusca", "Plantae", "Fungi"]

    def test_correct_number_of_outputs(self):
        results = group_by_taxa(self.df, self.upper_taxa)
        self.assertEqual(len(results), len(self.upper_taxa) + 1)

    def test_individual_taxa_dataframes(self):
        insecta_df, mollusca_df, plantae_df, fungi_df, others_df = group_by_taxa(
            self.df, self.upper_taxa
        )

        expected_insecta = (
            self.df[self.df["upper taxa"] == "Insecta"]
            .drop(columns=["upper taxa"])
            .reset_index(drop=True)
        )
        expected_mollusca = (
            self.df[self.df["upper taxa"] == "Mollusca"]
            .drop(columns=["upper taxa"])
            .reset_index(drop=True)
        )
        expected_plantae = (
            self.df[self.df["upper taxa"] == "Plantae"]
            .drop(columns=["upper taxa"])
            .reset_index(drop=True)
        )
        expected_fungi = (
            self.df[self.df["upper taxa"] == "Fungi"]
            .drop(columns=["upper taxa"])
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(insecta_df, expected_insecta)
        pd.testing.assert_frame_equal(mollusca_df, expected_mollusca)
        pd.testing.assert_frame_equal(plantae_df, expected_plantae)
        pd.testing.assert_frame_equal(fungi_df, expected_fungi)

    def test_others_dataframe(self):
        _, _, _, _, others_df = group_by_taxa(self.df, self.upper_taxa)

        expected_others = (
            self.df[~self.df["upper taxa"].isin(self.upper_taxa)]
            .drop(columns=["upper taxa"])
            .reset_index(drop=True)
        )

        if expected_others.empty:
            self.assertIsNone(others_df)
        else:
            pd.testing.assert_frame_equal(others_df, expected_others)

    def test_empty_dataframe(self):
        empty_df = pd.DataFrame(columns=["Species", "upper taxa", "Value"])
        results = group_by_taxa(empty_df, self.upper_taxa)

        for df in results:
            self.assertTrue(df is None or df.empty)

    def test_missing_taxa_column(self):
        df_no_taxa = self.df.drop(columns=["upper taxa"])
        with self.assertRaises(KeyError):
            group_by_taxa(df_no_taxa, self.upper_taxa)
