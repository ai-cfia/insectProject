from inaturalist_client import Configuration
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Species(BaseModel):
    name: str | None = None
    id: str | None = None


class SpeciesData(BaseModel):
    invasive: list[Species] | None = []
    non_invasive: list[Species] | None = []


class Area(BaseModel):
    nelat: float | None = None
    nelng: float | None = None
    swlat: float | None = None
    swlng: float | None = None


class AreaData(BaseModel):
    CA: Area | None = None
    US: Area | None = None


class Settings(BaseSettings):
    inat_host: str = "https://api.inaturalist.org/v1"
    api_request_delay: float = 3.0

    species_data: SpeciesData = SpeciesData(
        invasive=[
            Species(name="Asian Long-horned Beetle"),
            Species(name="Citrus Longhorn Beetle"),
        ],
        non_invasive=[Species(name="Monochamus scutellatus", id="82043")],
    )

    species_classification_model_path: str = "models/densenet_model_beta_AsianLonghorn"

    areas: AreaData = AreaData(
        CA=Area(nelat=83.1, nelng=-50.75, swlat=41.2833333, swlng=-140.8833333),
        US=Area(nelat=49.386847, nelng=-50.954936, swlat=25.489156, swlng=-128.826023),
    )

    def configuration(self) -> Configuration:
        return Configuration(host=self.inat_host, retries=5, debug=True)
