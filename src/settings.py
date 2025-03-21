from inaturalist_client import Configuration
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, EmailStr, SecretStr, computed_field
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


# TODO: limit the use of Settings to the main module
# TODO: proper logging


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
    upper_taxa: list[str] = ["Insecta", "Plantae", "Mollusca", "Fungi", "Chromista"]
    other_taxa_label: str = (
        "Others (recently added species which do not belong to the above) .."
    )
    areas: AreaData = AreaData(
        CA=Area(nelat=83.1, nelng=-50.75, swlat=41.2833333, swlng=-140.8833333),
        US=Area(nelat=49.386847, nelng=-50.954936, swlat=25.489156, swlng=-128.826023),
    )

    project_id: str = "91863"

    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: SecretStr
    smtp_debug_level: int = 0
    sender_email: EmailStr
    sender_name: str = "AI LAB CFIA"
    recipient_emails: list[EmailStr]
    email_template_dir: str = "templates"
    email_template_name: str = "email_template.html"

    @computed_field
    @property
    def inat_client_config(self) -> Configuration:
        return Configuration(host=self.inat_host, retries=5, debug=True)

    @property
    def template_loader(self) -> FileSystemLoader:
        return FileSystemLoader(self.email_template_dir)

    @property
    def template_env(self) -> Environment:
        return Environment(loader=self.template_loader, autoescape=True)
