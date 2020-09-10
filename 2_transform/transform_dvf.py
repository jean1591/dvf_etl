# PACKAGES
import argparse
import pandas as pd
import numpy as np
from progress.bar import ShadyBar
from datetime import datetime


def get_year():
    """
    Get year from args

    Returns:
        int: year to ETL
    """
    parser = argparse.ArgumentParser()

    # Retrieve year passed in args
    parser.add_argument("-y", "--year", help="DVF year")
    args = parser.parse_args()

    # Rename args
    return int(args.year)


def csv_to_df(year):
    """
    Load csv file to a DataFrame

    Args:
        year (int): Year of the file

    Returns:
        DataFrame: Full DataFrame on specified columns
    """
    # Specify columns to load
    cols_to_load = ["id_mutation", "date_mutation", "nature_mutation", "valeur_fonciere", "adresse_numero", "adresse_suffixe", "adresse_nom_voie", "code_postal",
                    "nom_commune", "code_departement", "type_local", "surface_reelle_bati", "nombre_pieces_principales", "longitude", "latitude"]

    # Specify columns types
    cols_types = {"id_mutation": str, "nature_mutation": str, "valeur_fonciere": float, "adresse_numero": "Int64", "adresse_suffixe": str, "adresse_nom_voie": str, "code_postal": str,
                  "nom_commune": str, "code_departement": str, "type_local": str, "surface_reelle_bati": float, "nombre_pieces_principales": float, "longitude": str, "latitude": str}

    # Read csv ad DataFrame with specified columns
    df = pd.read_csv(
        f"data/dvf_{year}.csv",
        usecols=cols_to_load,
        parse_dates=["date_mutation"],
        dtype=cols_types)

    # Rename columns
    df.columns = ["_idMutation", "createdAt", "typeOfSearch", "price", "streetNumber", "houseNumber", "streetName",
                  "postalCode", "city", "departement", "typeOfBuilding", "surface", "nbRoom", "longitude", "latitude"]

    return df


def drop_na(df):
    """
    Drop rows with na values on given columns

    Args:
        df (DataFrame): DataFrame in which to perform the dropna

    Returns:
        DataFrame: Update DataFrame
    """
    # DROP NA
    mandatory_rows = ["typeOfSearch", "price"]
    optional_rows = ["typeOfBuilding", "surface", "nbRoom"]
    df.dropna(subset=mandatory_rows, how="any", inplace=True)
    df.dropna(subset=optional_rows, how="all", inplace=True)

    return df


def update_fields_values(df):
    """
    Update values from specific columns

    Args:
        df (DataFrame): DataFrame on which to perform the modifications

    Returns:
        DataFrame: Update DataFrame
    """
    # Lowercase on all typeOfBuilding
    df["typeOfBuilding"] = df["typeOfBuilding"].str.lower()

    # Lowercase on all typeOfSearch & Update "vente" to "achat"
    df["typeOfSearch"] = df["typeOfSearch"].str.lower()
    df["typeOfSearch"] = np.where(
        df["typeOfSearch"] == "vente", "achat", df["typeOfSearch"])

    return df


def drop_useless_rows(df):
    """
    Drop rows with typeOfBuilding not in ["appartement", "maison"]

    Args:
        df (DataFrame): DataFrame on which to perform the modifications

    Returns:
        DataFrame: Update DataFrame
    """
    return df[df["typeOfBuilding"].isin(["appartement", "maison"])]


def validation(df):
    """
    Drop rows that does not match criterea

    Args:
        df (DataFrame): DataFrame on which to perform the modifications

    Returns:
        DataFrame: Update DataFrame
    """
    # Price / Surface / TypeOfSearch
    df = df[(df["price"] > 4999) & (df["price"] < 1999999)]

    # Surface
    df = df[(df["surface"] > 9) & (df["surface"] < 1001)]

    # TypeOfSearch
    df = df[df["typeOfSearch"] == "achat"]
    return df


def groupby(df):
    """
    Group by _idMutation, summing surface and nbRoom together and diplaying other field as such

    Args:
        df (DataFrame): DataFrame on which to perform the groupby

    Returns:
        DataFrame: Update DataFrame
    """
    return df.groupby("_idMutation", as_index=True).agg({
        "createdAt": "first", "typeOfSearch": "first", "price": "first", "streetNumber": "first", "houseNumber": "first", "streetName": "first",
        "postalCode": "first", "city": "first", "departement": "first", "typeOfBuilding": "first", "surface": "sum", "nbRoom": "sum", "longitude": "first", "latitude": "first"
    })


def save_df(df, year):
    """
    Save df as csv file with year in filename

    Args:
        df (DataFrame): DataFrame to save
        year (int): Year of the DataFrame
    """
    df.to_csv(f"data/dvf_{year}_updated.csv")


def main(timer=False):
    """
    Perform TRANSFORM on csv file with year passed as args
    """

    if timer:
        start_timer = datetime.now()

    # Init bar
    bar = ShadyBar(
        "Processing Data".ljust(25),
        max=6,
        width=50)

    year = get_year()

    dvf_DF = csv_to_df(year)
    bar.next()

    dvf_DF = drop_na(dvf_DF)
    bar.next()

    dvf_DF = update_fields_values(dvf_DF)
    bar.next()

    dvf_DF = drop_useless_rows(dvf_DF)
    bar.next()

    dvf_DF = validation(dvf_DF)
    bar.next()

    dvf_DF = groupby(dvf_DF)
    bar.next()

    save_df(dvf_DF, year)

    bar.finish()

    if timer:
        end_timer = datetime.now() - start_timer
        print(f"Execution time: {str(end_timer).split('.', 2)[0]}")


main(timer=True)
