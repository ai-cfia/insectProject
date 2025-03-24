import asyncio
import logging
from datetime import date, datetime

import pandas as pd
from inaturalist_client import ApiClient, Observation, ObservationsApi
from pydantic import validate_call

from src.custom_logging import log_call
from src.dates import get_yesterday
from src.pydantic_models import ObservationSummary, Region
from src.settings import Settings

log = logging.getLogger(__name__)


@log_call
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

    async with ApiClient(settings.inat_client_config) as api_client:
        api_instance = ObservationsApi(api_client)

        match region:
            case Region.CA:
                area = settings.areas.CA
            case Region.US:
                area = settings.areas.US
            case _:
                area = None

        return await api_instance.observations_get(
            per_page=str(per_page),
            page=str(page),
            created_d1=date_from,
            created_d2=date_to,
            created_on=date_on,
            taxon_name=taxon_names if taxon_names else None,
            taxon_id=[str(tid) for tid in taxon_ids] if taxon_ids else None,
            nelat=area.nelat if area else None,
            nelng=area.nelng if area else None,
            swlat=area.swlat if area else None,
            swlng=area.swlng if area else None,
        )


@log_call
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
            log.debug("No more observations to fetch.")
            break

        all_observations.extend(observations.results)
        fetched_count += len(observations.results)
        log.debug(f"Fetched {fetched_count} observations so far.")

        if len(observations.results) < per_page:
            log.debug("All observations retrieved.")
            break

        page += 1

    return all_observations


@log_call
@validate_call
def transform_summaries_to_df(
    summaries: list[ObservationSummary], column_mapping: dict[str, str]
) -> pd.DataFrame:
    if len(set(column_mapping.values())) != len(column_mapping.values()):
        raise ValueError("Duplicate output column names in mapping")

    model_keys = ObservationSummary(id=0).model_dump().keys()
    invalid_keys = set(column_mapping.keys()) - set(model_keys)
    if invalid_keys:
        raise KeyError(f"Invalid column mapping keys: {invalid_keys}")

    df = pd.DataFrame(columns=model_keys)

    if summaries:
        data = [summary.model_dump() for summary in summaries]
        df = pd.DataFrame(data)

    return df.rename(columns=column_mapping)


@log_call
@validate_call
async def get_yesterdays_observation_summaries_df(
    settings: Settings, taxon_ids: list[int], region: Region | None = Region.CA
):
    observations = await get_all_observations(
        settings=settings,
        taxon_ids=taxon_ids,
        date_on=get_yesterday(),
        region=region,
    )
    summaries = [
        ObservationSummary.model_validate(o.model_dump()) for o in observations
    ]
    return transform_summaries_to_df(summaries, settings.df_column_map_default)


if __name__ == "__main__":
    # run with `python -m src.observations`
    from dotenv import load_dotenv

    load_dotenv()
    settings = Settings()

    async def main():
        # taxon_ids = [47115]
        taxon_ids = [128525]
        region = None

        df = await get_yesterdays_observation_summaries_df(
            settings=settings,
            taxon_ids=taxon_ids,
            region=region,
        )

        print(df)

    asyncio.run(main())
