import logging

import pandas as pd
from pydantic import validate_call

from src.custom_logging import log_call
from src.emails import make_email_title, send_email
from src.models import PredictionLabel, load_densenet_model, predict_invasiveness
from src.observations import get_yesterdays_observation_summaries_df
from src.preprocess import clean_and_format_df, exclude_non_invasive
from src.settings import Settings
from src.species import get_specie_ids

log = logging.getLogger(__name__)


@log_call
@validate_call
async def generate_and_send_reports(s: Settings):
    regulated_taxon_ids = await get_specie_ids(s)
    email_title = make_email_title(s, not regulated_taxon_ids)

    model = load_densenet_model(s.species_classification_model_path)

    ca_observations_df = await get_yesterdays_observation_summaries_df(
        settings=s, taxon_ids=regulated_taxon_ids
    )

    us_observations_df = await get_yesterdays_observation_summaries_df(
        settings=s,
        taxon_ids=s.us_regulated_taxon_ids,
        region="US",
    )

    ca_observations_df[s.ml_column] = ""
    ca_observations_df = exclude_non_invasive(s, ca_observations_df)

    for non_invasive_specie in s.species_data.non_invasive:
        if not non_invasive_specie.id:
            continue
        df = await get_yesterdays_observation_summaries_df(
            s, taxon_ids=[non_invasive_specie.id]
        )
        print("here")
        print(df[s.image_column])
        df[s.ml_column] = predict_invasiveness(
            s, df[s.image_column], model, PredictionLabel.NON_INVASIVE
        )
        df = df[df[s.ml_column] == PredictionLabel.INVASIVE.value].reset_index(
            drop=True
        )
        ca_observations_df = pd.concat([ca_observations_df, df], ignore_index=True)

    for invasive_specie in s.species_data.invasive:
        if not invasive_specie.name:
            continue
        images = ca_observations_df[
            ca_observations_df[s.name_alt_column] == invasive_specie.name
        ][s.image_column]
        print("here")
        print(images)
        ca_observations_df.loc[
            ca_observations_df[s.name_alt_column] == invasive_specie.name, s.ml_column
        ] = predict_invasiveness(s, images, model, PredictionLabel.INVASIVE)

    regulated_ca_observations_df = clean_and_format_df(
        s, ca_observations_df, s.df_header_ca
    )
    print("here regulated_ca_observations_df")
    print(regulated_ca_observations_df)
    regulated_us_observations_df = clean_and_format_df(
        s, us_observations_df, s.df_header_us
    )

    send_email(
        s,
        regulated_ca_observations_df,
        regulated_us_observations_df,
        email_title,
    )


if __name__ == "__main__":
    # run with `python -m src.observation_reports`
    import asyncio
    import logging

    from dotenv import load_dotenv

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("geopy").setLevel(logging.WARNING)

    load_dotenv()
    settings = Settings()
    asyncio.run(generate_and_send_reports(settings))
