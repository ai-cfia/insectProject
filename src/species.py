import asyncio

from inaturalist_client import ApiClient, ProjectsApi
from pydantic import validate_call

from src.settings import Settings


@validate_call
async def get_specie_ids(settings: Settings):
    async with ApiClient(settings.configuration()) as api_client:
        results = (
            await ProjectsApi(api_client).projects_id_get(
                [settings.project_id], rule_details="true"
            )
        ).results
        if not results:
            return []
        return [
            r.taxon.id
            for r in results[0].project_observation_rules
            if r.taxon and r.taxon.id
        ]


if __name__ == "__main__":
    # run with python -m src.species
    import asyncio

    settings = Settings()

    ids = asyncio.run(get_specie_ids(settings))
    print(ids)
