from datetime import datetime
from enum import Enum

from pydantic import AliasChoices, AliasPath, BaseModel, Field, field_validator


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


class Region(Enum):
    CA = "CA"
    US = "US"


class ObservationSummary(BaseModel):
    id: int
    quality_grade: str | None = None
    name: str | None = Field(
        "",
        validation_alias=AliasChoices(
            "name", AliasPath("taxon", "preferred_common_name")
        ),
    )
    name_alt: str | None = Field(
        "", validation_alias=AliasChoices("name_alt", AliasPath("taxon", "name"))
    )
    uri: str | None = ""
    image_urls: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("image_urls", AliasPath("photos")),
    )
    coordinates: list[float] | None = Field(
        default_factory=list,
        validation_alias=AliasChoices(
            "coordinates", AliasPath("geojson", "coordinates")
        ),
    )
    created_at: datetime | None = Field(
        None,
        validation_alias=AliasChoices(
            "created_at", AliasPath("created_at_details", "var_date")
        ),
    )
    observed_at: datetime | None = Field(
        None, validation_alias=AliasChoices("observed_at", AliasPath("observed_on"))
    )
    username: str | None = Field(
        "", validation_alias=AliasChoices("username", AliasPath("user", "login"))
    )
    taxon_name: str | None = Field(
        "",
        validation_alias=AliasChoices(
            "taxon_name", AliasPath("taxon", "iconic_taxon_name")
        ),
    )
    taxon_id: int | None = Field(
        None,
        validation_alias=AliasChoices(
            "taxon_id", AliasPath("taxon", "id")
        ),
    )

    @field_validator("image_urls", mode="before")
    @classmethod
    def extract_image_urls(cls, value: list[str]):
        if isinstance(value, list):
            return [
                v["url"].replace("square", "large")
                if isinstance(v, dict) and "url" in v
                else v.replace("square", "large")
                if isinstance(v, str)
                else v
                for v in value
            ]
        return []
