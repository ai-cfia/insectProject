import asyncio
from datetime import date, datetime
from enum import Enum
from typing import Annotated

import pandas as pd
from inaturalist_client import ApiClient, Observation, ObservationsApi
from pydantic import (
    AliasChoices,
    AliasPath,
    BaseModel,
    Field,
    field_validator,
    validate_call,
)

from src.dates import get_yesterday
from src.settings import Settings


class Region(Enum):
    CA = "CA"
    US = "US"


class ObservationSummary(BaseModel):
    id: int
    quality_grade: str
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
            "taxon_id", AliasPath("taxon", "iconic_taxon_id")
        ),
    )

    @field_validator("image_urls", mode="before")
    @classmethod
    def extract_image_urls(cls, value):
        if isinstance(value, list):
            return [
                v["url"].replace("square", "large")
                if isinstance(v, dict) and "url" in v
                else v.replace("square", "large")  # TODO: right way to handle this?
                if isinstance(v, str)
                else v
                for v in value
            ]
        return []


# TODO: Annotate
# TODO: url safe query target
@validate_call
async def get_observations(
    settings: Settings,
    taxon_ids: list[int] | None = None,
    taxon_names: list[str] | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    date_on: date | None = None,
    region: Region | None = Region.CA,
    per_page: int = 200,
    page: int = 1,
):
    await asyncio.sleep(settings.api_request_delay)

    async with ApiClient(settings.configuration) as api_client:
        api_instance = ObservationsApi(api_client)

        nelat, nelng, swlat, swlng = (None, None, None, None)
        if region:
            nelat, nelng, swlat, swlng = settings.areas.get(
                region.value, (None, None, None, None)
            )

        return await api_instance.observations_get(
            per_page=str(per_page),
            page=str(page),
            created_d1=date_from,  # TODO: should it be create date or observe date?
            created_d2=date_to,
            created_on=date_on,
            taxon_name=taxon_names if taxon_names else None,
            taxon_id=[str(tid) for tid in taxon_ids] if taxon_ids else None,
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng,
        )


@validate_call
async def get_all_observations(
    settings: Settings,
    taxon_ids: list[int] | None = None,
    taxon_names: list[str] | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    date_on: date | None = None,
    region: Region | None = Region.CA,
    per_page: int = 200,
):
    all_observations: list[Observation] = []
    page = 1
    fetched_count = 0

    print("Starting observation retrieval...")

    while True:
        observations = await get_observations(
            settings=settings,
            taxon_ids=taxon_ids,
            taxon_names=taxon_names,
            date_from=date_from,
            date_to=date_to,
            date_on=date_on,
            region=region,
            per_page=per_page,
            page=page,
        )

        if not observations.results:
            print("No more observations to fetch. Exiting.")
            break

        all_observations.extend(observations.results)
        fetched_count += len(observations.results)

        print(f"Fetched {fetched_count} observations...")

        if len(observations.results) < per_page:
            print("All observations retrieved successfully.")
            break

        page += 1

    return all_observations


@validate_call
def transform_summaries_to_dataframe(
    summaries: Annotated[list[ObservationSummary], Field(..., min_length=1)],
    columns: list[str],
) -> pd.DataFrame:
    data = [summary.model_dump() for summary in summaries]
    df = pd.DataFrame(data)
    column_mapping = dict(zip(df.columns, columns))
    return df.rename(columns=column_mapping)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    settings = Settings()

    async def main():
        taxon_ids = [47115]
        date_on = get_yesterday()
        region = "CA"

        print("Fetching all observations...")
        all_observations = await get_all_observations(
            settings=settings,
            taxon_ids=taxon_ids,
            date_on=date_on,
            region=region,
        )

        print(f"Total observations retrieved: {len(all_observations)}")

        observation_summaries = [
            ObservationSummary.model_validate(obs.model_dump())
            for obs in all_observations
        ]

        columns = [
            "Obsvn_ID",
            "Quality",
            "Name1",
            "Name2",
            "Obsvn_URL",
            "Sample_Img",
            "coordinates",
            "posted_on",
            "observed_on",
            "user_login",
            "upper taxa",
            "upper_taxa_id",
        ]
        df = transform_summaries_to_dataframe(observation_summaries, columns)
        print(df)

    asyncio.run(main())
