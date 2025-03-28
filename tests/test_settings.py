import os
import unittest

from jinja2 import TemplateNotFound
from pydantic import computed_field

from src.settings import AppEnvironment, Area, AreaData, Settings, Species, SpeciesData


class TestSettings(unittest.TestCase):
    def setUp(self):
        os.environ["SPECIES_DATA"] = (
            '{"invasive":[{"name":"Asian Long-horned Beetle","id":"123"},'
            '{"name":"Citrus Longhorn Beetle","id":"456"}],'
            '"non_invasive":[{"name":"Monochamus scutellatus","id":"123456"}]}'
        )
        os.environ["AREAS"] = (
            '{"CA":{"nelat":83.1,"nelng":-50.75,"swlat":41.2833333,"swlng":-140.8833333},'
            '"US":{"nelat":49.386847,"nelng":-50.954936,"swlat":25.489156,"swlng":-128.826023}}'
        )
        os.environ["SMTP_HOST"] = "smtp.example.com"
        os.environ["SMTP_PORT"] = "587"
        os.environ["SMTP_USERNAME"] = "user"
        os.environ["SMTP_PASSWORD"] = "pass"
        os.environ["SENDER_EMAIL"] = "a@example.com"
        os.environ["OBSERVATIONS_EMAIL_RECIPIENTS"] = (
            '["b@example.com", "c@example.com"]'
        )
        os.environ["COMMENTS_EMAIL_RECIPIENTS"] = '["d@example.com"]'
        os.environ["ENVIRONMENT"] = "dev"
        self.settings = Settings()

    def tearDown(self):
        keys = [
            "SPECIES_DATA",
            "AREAS",
            "SMTP_HOST",
            "SMTP_PORT",
            "SMTP_USERNAME",
            "SMTP_PASSWORD",
            "SENDER_EMAIL",
            "OBSERVATIONS_EMAIL_RECIPIENTS",
            "COMMENTS_EMAIL_RECIPIENTS",
            "ENVIRONMENT",
        ]
        for k in keys:
            os.environ.pop(k, None)

    def test_environment(self):
        self.assertEqual(self.settings.environment, AppEnvironment.DEVELOPMENT)

    def test_species_data(self):
        expected = SpeciesData(
            invasive=[
                Species(name="Asian Long-horned Beetle", id="123"),
                Species(name="Citrus Longhorn Beetle", id="456"),
            ],
            non_invasive=[Species(name="Monochamus scutellatus", id="123456")],
        )
        self.assertEqual(self.settings.species_data, expected)

    def test_areas_data(self):
        expected = AreaData(
            CA=Area(nelat=83.1, nelng=-50.75, swlat=41.2833333, swlng=-140.8833333),
            US=Area(
                nelat=49.386847, nelng=-50.954936, swlat=25.489156, swlng=-128.826023
            ),
        )
        self.assertEqual(self.settings.areas, expected)

    def test_missing_species_data(self):
        os.environ.pop("SPECIES_DATA", None)
        settings = Settings()
        expected = SpeciesData(
            invasive=[
                Species(name="Asian Long-horned Beetle"),
                Species(name="Citrus Longhorn Beetle"),
            ],
            non_invasive=[Species(name="Monochamus scutellatus", id="82043")],
        )
        self.assertEqual(settings.species_data, expected)

    def test_missing_areas_data(self):
        os.environ.pop("AREAS", None)
        settings = Settings()
        expected = AreaData(
            CA=Area(nelat=83.1, nelng=-50.75, swlat=41.2833333, swlng=-140.8833333),
            US=Area(
                nelat=49.386847, nelng=-50.954936, swlat=25.489156, swlng=-128.826023
            ),
        )
        self.assertEqual(settings.areas, expected)

    def test_recipient_emails_parsing(self):
        self.assertEqual(
            self.settings.observations_email_recipients,
            ["b@example.com", "c@example.com"],
        )
        self.assertEqual(
            self.settings.comments_email_recipients,
            ["d@example.com"],
        )

    def test_computed_field_evaluation_raises(self):
        class BadSettings(Settings):
            observations_email_subject_template_name: str = "nonexistent_template.j2"

        with self.assertRaises(TemplateNotFound):
            BadSettings()

    def test_invalid_column_map_key_raises(self):
        class BadSettings(Settings):
            @computed_field
            @property
            def df_column_map_default(self) -> dict[str, str]:
                return {"invalid_key": "some_column"}

        with self.assertRaises(ValueError) as ctx:
            BadSettings()
        self.assertIn("Invalid df_column_map_default keys", str(ctx.exception))
