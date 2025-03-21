import io
import os
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path

import torchvision.transforms as transforms
from PIL import Image
from pydantic import HttpUrl, validate_call
from torch import Tensor


@contextmanager
@validate_call
def download(url: HttpUrl):
    filename = Path(f"{uuid.uuid4().hex}.jpg")

    try:
        with urllib.request.urlopen(str(url)) as response:
            image_data = io.BytesIO(response.read())

        image = Image.open(image_data)

        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")

        image.save(filename)
        yield filename

    finally:
        if filename.exists():
            os.remove(filename)


@validate_call
def preprocess_image(image_bytes: bytes):
    # TODO: hardcoded values
    transform = transforms.Compose(
        [
            transforms.Resize(255),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
        ]
    )
    image = Image.open(io.BytesIO(image_bytes))
    tensor: Tensor = transform(image)
    return tensor.unsqueeze(0)


if __name__ == "__main__":
    # Run with "python -m src.images"
    test_url = (
        "https://upload.wikimedia.org/wikipedia/commons/3/3f/JPEG_example_flower.jpg"
    )

    # Test downloading and preprocessing
    with download(test_url) as test_filename:
        assert test_filename.exists(), "Downloaded image file does not exist."

        with open(test_filename, "rb") as f:
            image_bytes = f.read()

        tensor = preprocess_image(image_bytes)
        assert isinstance(tensor, Tensor) and tensor.shape == (1, 3, 224, 224)

    print("All tests passed.")
