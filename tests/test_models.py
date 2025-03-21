import unittest
from unittest.mock import MagicMock, mock_open, patch

import torch
from torchvision.models import DenseNet

from src.models import PredictionLabel, predict_invasiveness


class TestPredictInvasiveness(unittest.TestCase):
    def setUp(self):
        self.mock_model = MagicMock(spec=DenseNet)
        self.mock_model.forward.return_value = torch.tensor(
            [[0.2, 0.8]]
        )  # Default: NON_INVASIVE
        self.image_sets = [
            ["http://example.com/image1.jpg"],  # Single image
            [
                "http://example.com/image2.jpg",
                "http://example.com/image3.jpg",
            ],  # Multiple images
            ["http://example.com/copyrighted_image.jpg"],  # Copyrighted image
            [
                "http://example.com/invasive1.jpg",
                "http://example.com/non_invasive.jpg",
            ],  # INVASIVE appears first
        ]
        self.default_prediction = PredictionLabel.NON_INVASIVE

    @patch("src.models.download")
    @patch("src.models.preprocess_image")
    def test_basic_prediction(self, mock_preprocess, mock_download):
        mock_preprocess.return_value = torch.zeros((1, 3, 224, 224))
        mock_download.return_value.__enter__.return_value = "dummy_path"
        with patch("builtins.open", mock_open(read_data=b"image_data")):
            preds = predict_invasiveness(
                self.image_sets[:2], self.mock_model, self.default_prediction
            )
        self.assertEqual(len(preds), 2)
        self.assertIn(
            preds[0],
            [PredictionLabel.INVASIVE.value, PredictionLabel.NON_INVASIVE.value],
        )

    def test_copyrighted_image(self):
        preds = predict_invasiveness(
            [self.image_sets[2]], self.mock_model, self.default_prediction
        )
        self.assertEqual(preds, [PredictionLabel.REMOVED_COPYRIGHT.value])

    @patch("src.models.download")
    @patch("src.models.preprocess_image")
    def test_early_break_on_invasive(self, mock_preprocess, mock_download):
        self.mock_model.forward.side_effect = [
            torch.tensor([[0.9, 0.1]]),  # INVASIVE
            torch.tensor([[0.2, 0.8]]),  # Should not be reached
        ]
        mock_preprocess.return_value = torch.zeros((1, 3, 224, 224))
        mock_download.return_value.__enter__.return_value = "dummy_path"
        with patch("builtins.open", mock_open(read_data=b"image_data")):
            preds = predict_invasiveness(
                [self.image_sets[3]], self.mock_model, self.default_prediction
            )
        self.assertEqual(preds, [PredictionLabel.INVASIVE.value])

    @patch("src.models.download")
    @patch("src.models.preprocess_image")
    def test_model_output_mapping(self, mock_preprocess, mock_download):
        self.mock_model.forward.side_effect = [
            torch.tensor([[0.1, 0.9]]),  # NON_INVASIVE
            torch.tensor([[0.8, 0.2]]),  # INVASIVE
        ]
        mock_preprocess.return_value = torch.zeros((1, 3, 224, 224))
        mock_download.return_value.__enter__.return_value = "dummy_path"
        with patch("builtins.open", mock_open(read_data=b"image_data")):
            preds = predict_invasiveness(
                [self.image_sets[1]], self.mock_model, self.default_prediction
            )
        self.assertEqual(preds, [PredictionLabel.INVASIVE.value])  # Breaks on INVASIVE

    @patch("src.models.download")
    @patch("src.models.preprocess_image")
    def test_empty_model_output(self, mock_preprocess, mock_download):
        self.mock_model.forward.return_value = torch.tensor([])
        mock_preprocess.return_value = torch.zeros((1, 3, 224, 224))
        mock_download.return_value.__enter__.return_value = "dummy_path"
        with patch("builtins.open", mock_open(read_data=b"image_data")):
            preds = predict_invasiveness(
                [self.image_sets[0]], self.mock_model, self.default_prediction
            )
        self.assertEqual(preds, [self.default_prediction.value])

    @patch("src.models.download")
    @patch("src.models.preprocess_image")
    def test_no_break_if_all_non_invasive(self, mock_preprocess, mock_download):
        self.mock_model.forward.side_effect = [
            torch.tensor([[0.1, 0.9]]),
            torch.tensor([[0.2, 0.8]]),
        ]
        mock_preprocess.return_value = torch.zeros((1, 3, 224, 224))
        mock_download.return_value.__enter__.return_value = "dummy_path"
        with patch("builtins.open", mock_open(read_data=b"image_data")):
            preds = predict_invasiveness(
                [self.image_sets[1]], self.mock_model, self.default_prediction
            )
        self.assertEqual(preds, [PredictionLabel.NON_INVASIVE.value])

    @patch("src.models.download")
    @patch("src.models.preprocess_image")
    def test_no_processing_after_copyrighted(self, mock_preprocess, mock_download):
        mock_preprocess.return_value = torch.zeros((1, 3, 224, 224))
        mock_download.return_value.__enter__.return_value = "dummy_path"
        with patch("builtins.open", mock_open(read_data=b"image_data")):
            preds = predict_invasiveness(
                [self.image_sets[2], self.image_sets[0]],
                self.mock_model,
                self.default_prediction,
            )
        self.assertEqual(
            preds,
            [
                PredictionLabel.REMOVED_COPYRIGHT.value,
                PredictionLabel.NON_INVASIVE.value,
            ],
        )
