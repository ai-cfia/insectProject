import unittest
from pathlib import Path

from src.images import download, preprocess_image
from tests import settings


class TestImageUtils(unittest.TestCase):
    def setUp(self):
        self.settings = settings.model_copy()
        self.test_url = "https://upload.wikimedia.org/wikipedia/commons/3/3f/JPEG_example_flower.jpg"

    def test_download_creates_and_removes_file(self):
        with download(self.test_url) as filename:
            self.assertTrue(Path(filename).exists())
        self.assertFalse(Path(filename).exists())

    def test_preprocess_image_tensor_shape(self):
        with download(self.test_url) as filename:
            with open(filename, "rb") as f:
                image_bytes = f.read()
            tensor = preprocess_image(self.settings, image_bytes)
            self.assertEqual(tuple(tensor.shape), (1, 3, 224, 224))
