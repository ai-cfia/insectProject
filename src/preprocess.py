import pandas as pd

from src.geo import get_city_province_country


def filter_north_american_locations(df: pd.DataFrame):
    if df.empty:
        return df[["coordinates"]].drop(columns=["coordinates"])

    locations = [get_city_province_country(coord) for coord in df["coordinates"]]

    if not locations:
        return df[["coordinates"]].drop(columns=["coordinates"])

    cities, provinces, countries = zip(*locations) if locations else ([], [], [])

    df["City"] = cities
    df["Province"] = provinces
    df["Country"] = [c.lower() for c in countries]

    df = df.loc[df["Country"].isin({"ca", "us"})].copy()

    df.drop(columns=["coordinates", "Country"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def extract_first_sample_image(df: pd.DataFrame):
    if "Sample_Img" not in df.columns:
        return df

    for i in df.index:
        images = df.at[i, "Sample_Img"]
        df.at[i, "Sample_Img"] = images[0] if images and len(images) > 0 else None
    return df


def split_by_taxonomy(df_regulated: pd.DataFrame, upper_taxa: list[str]):
    col = "upper taxa"
    taxa_dfs = {
        taxon: df_regulated[df_regulated[col] == taxon]
        .drop(columns=[col])
        .reset_index(drop=True)
        for taxon in upper_taxa
    }

    df_others = df_regulated[~df_regulated[col].isin(upper_taxa)]
    df_others = (
        df_others.drop(columns=[col]).reset_index(drop=True)
        if not df_others.empty
        else None
    )

    return (*taxa_dfs.values(), df_others)


if __name__ == "__main__":
    # Run with `python -m src.preprocess`
    test_data = {
        "coordinates": [
            (45.4215, -75.6993),  # Ottawa, CA
            (40.7128, -74.0060),  # New York, US
            (48.8566, 2.3522),  # Paris, FR (should be filtered out)
        ],
        "Sample_Img": [
            ["image1.jpg", "image2.jpg"],  # List of images
            ["image3.jpg"],  # Single image in list
            [],  # Empty list
        ],
    }

    df = pd.DataFrame(test_data)

    # Test filter_north_american_locations
    filtered_df = filter_north_american_locations(df)
    print("Filtered DataFrame:")
    print(filtered_df)

    # Test extract_first_sample_image
    updated_df = extract_first_sample_image(filtered_df)
    print("\nUpdated DataFrame with Sample_Img processing:")
    print(updated_df)

    # Test data for split_by_taxonomy
    test_taxa_data = {
        "Species": ["Species1", "Species2", "Species3", "Species4", "Species5"],
        "upper taxa": ["Insecta", "Mollusca", "Plantae", "Fungi", "Unknown"],
        "Value": [10, 20, 30, 40, 50],
    }

    df_taxa = pd.DataFrame(test_taxa_data)
    upper_taxa_list = ["Insecta", "Mollusca", "Plantae", "Fungi"]

    # Test split_by_taxonomy
    insecta_df, mollusca_df, plantae_df, fungi_df, others_df = split_by_taxonomy(
        df_taxa, upper_taxa_list
    )

    print("\nInsecta DataFrame:")
    print(insecta_df)

    print("\nMollusca DataFrame:")
    print(mollusca_df)

    print("\nPlantae DataFrame:")
    print(plantae_df)

    print("\nFungi DataFrame:")
    print(fungi_df)

    print("\nOthers DataFrame:")
    print(others_df)
