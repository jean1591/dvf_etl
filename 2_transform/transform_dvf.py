# PACKAGES
import argparse
import pandas as pd
import numpy as np
from progress.bar import ShadyBar


def get_year():
    parser = argparse.ArgumentParser()

    # Retrieve year passed in args
    parser.add_argument("-y", "--year", help="DVF year")
    args = parser.parse_args()

    # Rename args
    return int(args.year)


def csv_to_df(year):
    # CSV TO DATAFRAME
    # Specify columns to load
    cols_to_load = ["id_mutation", "date_mutation", "nature_mutation", "valeur_fonciere", "adresse_numero", "adresse_suffixe", "adresse_nom_voie", "code_postal",
                    "nom_commune", "code_departement", "id_parcelle", "type_local", "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "longitude", "latitude"]

    # Specify columns types
    cols_types = {"id_mutation": str, "nature_mutation": str, "valeur_fonciere": float, "adresse_numero": "Int64", "adresse_suffixe": str, "adresse_nom_voie": str, "code_postal": str,
                  "nom_commune": str, "code_departement": str, "id_parcelle": str, "type_local": str, "surface_reelle_bati": float, "nombre_pieces_principales": float, "surface_terrain": float, "longitude": str, "latitude": str}

    # Read csv ad DataFrame with specified columns
    df = pd.read_csv(
        f"data/dvf_{year}.csv",
        usecols=cols_to_load,
        parse_dates=["date_mutation"],
        dtype=cols_types)

    # Rename columns
    df.columns = ["_idMutation", "createdAt", "typeOfSearch", "price", "streetNumber", "houseNumber", "streetName",
                  "postalCode", "city", "departement", "plotId", "typeOfBuilding", "surface", "nbRoom", "surfacePlot", "longitude", "latitude"]

    return df


def drop_na(df):
    # DROP NA
    mandatory_rows = ["typeOfSearch", "price"]
    optional_rows = ["typeOfBuilding", "surface", "nbRoom"]
    df.dropna(subset=mandatory_rows, how="any", inplace=True)
    df.dropna(subset=optional_rows, how="all", inplace=True)

    return df


def update_fields_values(df):
    # UPDATE FIELDS VALUES
    # typeOfBuilding
    df["typeOfBuilding"] = df["typeOfBuilding"].str.lower()

    # typeOfSearch
    df["typeOfSearch"] = df["typeOfSearch"].str.lower()
    df["typeOfSearch"] = np.where(
        df["typeOfSearch"] == "vente", "achat", df["typeOfSearch"])

    return df


def drop_useless_rows(df):
    # DROP USELESS ROWS
    df = df[df["typeOfBuilding"].isin(["appartement", "maison"])]
    return df


def validation(df):
    # VALIDATION
    # Price / Surface / TypeOfSearch
    df = df[(df["price"] > 4999) & (df["price"] < 1999999)]

    # Surface
    df = df[(df["surface"] > 9) & (df["surface"] < 1001)]

    # TypeOfSearch
    df = df[df["typeOfSearch"] == "achat"]
    return df


def save_df(df, year):
    # SAVE
    df.to_csv(f"data/dvf_{year}_updated.csv")


def main():

    # Init bar
    bar = ShadyBar(
        "Processing Data".ljust(25),
        max=5,
        width=50)

    year = get_year()

    dvf_DF = csv_to_df(year)
    bar.next()

    dvf_DF = drop_na(dvf_DF)
    bar.next()

    update_fields_values(dvf_DF)
    bar.next()

    drop_useless_rows(dvf_DF)
    bar.next()

    validation(dvf_DF)
    bar.next()

    save_df(dvf_DF, year)

    bar.finish()


main()
