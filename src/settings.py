from inaturalist_client import Configuration
from jinja2 import Environment, FileSystemLoader
from pydantic import EmailStr, SecretStr, computed_field, model_validator
from pydantic_settings import BaseSettings

from src.pydantic_models import Area, AreaData, ObservationSummary, Species, SpeciesData


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
    us_regulated_taxon_ids: list[str] = ["324726", "325295", "471260", "497733"]
    ml_column: str = "ai_visionVerification"
    name_column: str = "Common Name"
    name_alt_column: str = "Scientific Name"
    image_column: str = "Image URLs"
    observation_id_column: str = "Observation ID"
    observation_url_column: str = "Observation URL"
    coords_column: str = "Coordinates"
    posted_on_column: str = "Posted On"
    observed_on_column: str = "Observed On"
    user_login_column: str = "User"
    upper_taxa_column: str = "Taxon Group"
    upper_taxa_id_column: str = "Taxon ID"
    city_column: str = "City"
    province_column: str = "Province"
    quality_column: str = "Quality Grade"

    @computed_field
    @property
    def df_column_map_default(self) -> dict[str, str]:
        return {
            "id": self.observation_id_column,
            "quality_grade": self.quality_column,
            "name": self.name_column,
            "name_alt": self.name_alt_column,
            "uri": self.observation_url_column,
            "image_urls": self.image_column,
            "coordinates": self.coords_column,
            "created_at": self.posted_on_column,
            "observed_at": self.observed_on_column,
            "username": self.user_login_column,
            "taxon_name": self.upper_taxa_column,
            "taxon_id": self.upper_taxa_id_column,
        }

    @computed_field
    @property
    def df_header_us(self) -> list[str]:
        return [
            self.observation_id_column,
            self.quality_column,
            self.name_column,
            self.name_alt_column,
            self.city_column,
            self.province_column,
            self.observation_url_column,
            self.image_column,
            self.posted_on_column,
            self.observed_on_column,
            self.user_login_column,
            self.upper_taxa_column,
        ]

    @computed_field
    @property
    def df_header_ca(self) -> list[str]:
        return self.df_header_us + [self.ml_column]

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
    email_title_prefix: str = "Observations of Invasive Species Submitted"
    email_maintenance_msg: str = (
        "error: website is temporarily disabled due to maintenance"
    )
    email_title_template: str = (
        "{{ prefix }} on {{ date }}{% if error %} — {{ error_msg }}{% endif %}"
    )

    @computed_field
    @property
    def inat_client_config(self) -> Configuration:
        return Configuration(host=self.inat_host, retries=5)

    @computed_field
    @property
    def template_loader(self) -> FileSystemLoader:
        return FileSystemLoader(self.email_template_dir)

    @computed_field
    @property
    def template_env(self) -> Environment:
        return Environment(loader=self.template_loader, autoescape=True)

    @model_validator(mode="after")
    def validate_column_map(self) -> "Settings":
        valid_keys = ObservationSummary.model_fields.keys()
        invalid = set(self.df_column_map_default.keys()) - set(valid_keys)
        if invalid:
            raise ValueError(f"Invalid df_column_map_default keys: {invalid}")
        return self

    image_resize: int = 255
    image_crop_size: int = 224
    image_normalize_mean_rgb: tuple[float, float, float] = (0.485, 0.456, 0.406)
    image_normalize_std_rgb: tuple[float, float, float] = (0.229, 0.224, 0.225)
