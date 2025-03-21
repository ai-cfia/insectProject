import os
import unittest

from src.settings import Area, AreaData, Settings, Species, SpeciesData


class TestSettings(unittest.TestCase):
    def setUp(self):
        os.environ["SPECIES_DATA"] = (
            '{"invasive":[{"name":"Asian Long-horned Beetle","id":"123"},{"name":"Citrus Longhorn Beetle","id":"456"}],"non_invasive":[{"name":"Monochamus scutellatus","id":"123456"}]}'
        )
        os.environ["AREAS"] = (
            '{"CA":{"nelat":83.1,"nelng":-50.75,"swlat":41.2833333,"swlng":-140.8833333},"US":{"nelat":49.386847,"nelng":-50.954936,"swlat":25.489156,"swlng":-128.826023}}'
        )
        self.settings = Settings()

    def test_species_data(self):
        expected = SpeciesData(
            invasive=[
                Species(name="Asian Long-horned Beetle", id="123"),
                Species(name="Citrus Longhorn Beetle", id="456"),
            ],
            non_invasive=[Species(name="Monochamus scutellatus", id="123456")],
        )
        self.assertEqual(self.settings.species_data, expected, self.settings.species_data)

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
