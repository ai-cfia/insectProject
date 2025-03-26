import asyncio
import logging
from datetime import date, datetime

import pandas as pd
from inaturalist_client import ApiClient, Observation, ObservationsApi
from pydantic import validate_call
from tqdm.asyncio import tqdm

from src.custom_logging import log_call
from src.dates import get_yesterday
from src.pydantic_models import Area, ObservationSummary
from src.settings import Settings

log = logging.getLogger(__name__)


@log_call
@validate_call
async def get_observations(
    s: Settings,
    taxon_ids: list[int] | None = None,
    taxon_names: list[str] | None = None,
    iconic_taxa: list[str] | None = None,
    order: str | None = None,
    order_by: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    date_on: date | None = None,
    per_page: int = 200,
    page: int = 1,
    area: Area | None = None,
):
    """Fetch a single page of observations from iNaturalist API"""
    await asyncio.sleep(s.api_request_delay)

    async with ApiClient(s.inat_client_config) as api_client:
        api_instance = ObservationsApi(api_client)
        return await api_instance.observations_get(
            per_page=str(per_page),
            page=str(page),
            created_d1=date_from,
            created_d2=date_to,
            created_on=date_on,
            taxon_name=taxon_names if taxon_names else None,
            taxon_id=[str(tid) for tid in taxon_ids] if taxon_ids else None,
            iconic_taxa=iconic_taxa,
            order=order,
            order_by=order_by,
            nelat=area.nelat if area else None,
            nelng=area.nelng if area else None,
            swlat=area.swlat if area else None,
            swlng=area.swlng if area else None,
        )


@log_call
@validate_call
async def get_all_observations(
    s: Settings,
    taxon_ids: list[int] | None = None,
    taxon_names: list[str] | None = None,
    iconic_taxa: list[str] | None = None,
    order: str | None = None,
    order_by: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    date_on: date | None = None,
    per_page: int = 200,
    area: Area | None = None,
):
    """Fetch all observations matching criteria with pagination"""
    # Get first page to determine total results
    first_page = await get_observations(
        s=s,
        taxon_ids=taxon_ids,
        taxon_names=taxon_names,
        iconic_taxa=iconic_taxa,
        order=order,
        order_by=order_by,
        date_from=date_from,
        date_to=date_to,
        date_on=date_on,
        per_page=1,
        page=1,
        area=area,
    )

    total_results = first_page.total_results if first_page.results else 0
    if total_results == 0:
        log.debug("No observations found.")
        return []

    all_observations: list[Observation] = []
    page = 1

    # Fetch all pages with progress bar
    with tqdm(total=total_results, desc="Fetching observations") as pbar:
        while True:
            observations = await get_observations(
                s=s,
                taxon_ids=taxon_ids,
                taxon_names=taxon_names,
                iconic_taxa=iconic_taxa,
                order=order,
                order_by=order_by,
                date_from=date_from,
                date_to=date_to,
                date_on=date_on,
                per_page=per_page,
                page=page,
                area=area,
            )

            if not observations.results:
                log.debug("No more observations to fetch.")
                break

            all_observations.extend(observations.results)
            pbar.update(len(observations.results))

            if len(all_observations) >= total_results:
                log.debug("All observations retrieved.")
                break

            page += 1

    return all_observations


@log_call
@validate_call
def transform_summaries_to_df(
    summaries: list[ObservationSummary], column_mapping: dict[str, str]
) -> pd.DataFrame:
    """Convert observation summaries to DataFrame with mapped column names"""
    # Validate column mapping
    if len(set(column_mapping.values())) != len(column_mapping.values()):
        raise ValueError("Duplicate output column names in mapping")

    model_keys = ObservationSummary(id=0).model_dump().keys()
    invalid_keys = set(column_mapping.keys()) - set(model_keys)
    if invalid_keys:
        raise KeyError(f"Invalid column mapping keys: {invalid_keys}")

    df = pd.DataFrame(columns=model_keys)

    # Convert summaries to DataFrame rows
    if summaries:
        data = [summary.model_dump() for summary in summaries]
        df = pd.DataFrame(data)

    return df.rename(columns=column_mapping)


@log_call
@validate_call
async def get_observation_summaries_df(
    s: Settings,
    taxon_ids: list[int],
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    date_on: date | None = None,
    area: Area | None = None,
):
    """Fetch observations and return as DataFrame with summary data"""
    if not taxon_ids:
        return transform_summaries_to_df([], s.df_column_map_default)

    observations = await get_all_observations(
        s=s,
        taxon_ids=taxon_ids,
        date_from=date_from,
        date_to=date_to,
        date_on=date_on,
        area=area,
    )
    summaries = [
        ObservationSummary.model_validate(o.model_dump()) for o in observations
    ]
    return transform_summaries_to_df(summaries, s.df_column_map_default)


if __name__ == "__main__":
    # run with `python -m src.observations`
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.DEBUG)

    load_dotenv()
    settings = Settings()

    async def main():
        taxon_ids = [47115]

        df = await get_observation_summaries_df(
            s=settings, taxon_ids=taxon_ids, date_on=get_yesterday()
        )

        print(df)

    asyncio.run(main())
