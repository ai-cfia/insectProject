import logging

from inaturalist_client import ApiClient, ProjectsApi
from pydantic import validate_call

from src.custom_logging import log_call
from src.settings import Settings

log = logging.getLogger(__name__)


@log_call
@validate_call
async def get_specie_ids(s: Settings):
    """Get list of species IDs from project observation rules"""
    async with ApiClient(s.inat_client_config) as api_client:
        # Get project details with observation rules
        results = (
            await ProjectsApi(api_client).projects_id_get(
                [s.project_id], rule_details="true"
            )
        ).results
        if not results:
            log.debug("No project results found.")
            return []

        # Extract taxon IDs from rules that have them
        ids = [
            r.taxon.id
            for r in results[0].project_observation_rules
            if r.taxon and r.taxon.id
        ]
        return ids


if __name__ == "__main__":
    # Example usage: python -m src.species
    import asyncio

    settings = Settings()

    ids = asyncio.run(get_specie_ids(settings))
    print(ids)
