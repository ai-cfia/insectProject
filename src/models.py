from enum import Enum

import torch
from pydantic import validate_call
from torch.nn import Linear
from torchvision.models import DenseNet, densenet121

from src.images import download, preprocess_image


@validate_call
def load_densenet_model(checkpoint_path: str):
    device = torch.device("cpu")

    model = densenet121(weights=None)
    model.classifier = Linear(model.classifier.in_features, 2)

    checkpoint = torch.load(checkpoint_path, map_location=device)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    return model


class PredictionLabel(Enum):
    INVASIVE = "invasive"
    NON_INVASIVE = "non_invasive"
    REMOVED_COPYRIGHT = "REMOVED-COPYRIGHT"


@validate_call(config=dict(arbitrary_types_allowed=True))
def predict_invasiveness(
    image_sets: list[list[str]], model: DenseNet, default_prediction: PredictionLabel
):
    class_index = {0: PredictionLabel.INVASIVE, 1: PredictionLabel.NON_INVASIVE}
    predictions: list[str] = []

    for image_set in image_sets:
        if any("copyright" in url.lower() for url in image_set):
            predictions.append(PredictionLabel.REMOVED_COPYRIGHT.value)
            continue

        prediction = default_prediction

        for image_url in image_set:
            with download(image_url) as f, open(f, "rb") as image:
                tensor = preprocess_image(image.read())
                output = model.forward(tensor)
                if output.numel() == 0:
                    continue
                _, y_hat = output.max(1)
                prediction = class_index[y_hat.item()]

            if prediction == PredictionLabel.INVASIVE:
                break  # Stop processing this set

        predictions.append(prediction.value)

    return predictions


if __name__ == "__main__":
    # Run with "python -m src.models"
    model_path = "models/densenet_model_beta_AsianLonghorn"
    model = load_densenet_model(model_path)

    # Test image sets (replace with actual URLs if needed)
    test_image_sets = [
        ["https://upload.wikimedia.org/wikipedia/commons/3/3f/JPEG_example_flower.jpg"],
        [
            "https://upload.wikimedia.org/wikipedia/commons/4/47/PNG_transparency_demonstration_1.png"
        ],
        [
            "https://example.com/copyrighted_image.jpg"
        ],  # Should trigger REMOVED_COPYRIGHT
    ]

    initial_prediction = PredictionLabel.NON_INVASIVE

    predictions = predict_invasiveness(test_image_sets, model, "non_invasive")

    for idx, (image_set, prediction) in enumerate(zip(test_image_sets, predictions)):
        print(f"Image Set {idx + 1}: {image_set} -> Prediction: {prediction}")
