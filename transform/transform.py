# IMPORTS
import logging
import pandas as pd
import numpy as np
from progress.bar import ShadyBar



def csv_to_df(year):
  """
  Load csv file to a DataFrame

  Args:
      year (str): Year of the file

  Returns:
      DataFrame: Full DataFrame on specified columns
  """

  logging.info(f"Loading dvf_{year}.csv to DataFrame")

  try:
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
  except FileNotFoundError:
    logging.error("... Failed >> FileNotFoundError")
    raise
  except Exception:
    logging.error("... Failed")
    raise


def drop_na(df):
  """
  Drop rows with na values on given columns

  Args:
      df (DataFrame): DataFrame in which to perform the dropna

  Returns:
      DataFrame: Update DataFrame
  """

  logging.info(f"Dropping NaN rows")

  mandatory_rows = ["typeOfSearch", "price"]
  optional_rows = ["typeOfBuilding", "surface", "nbRoom"]

  try:
    df.dropna(subset=mandatory_rows, how="any", inplace=True)
    df.dropna(subset=optional_rows, how="all", inplace=True)

    return df
  except Exception:
    logging.error("... Failed")
    raise


def update_fields_values(df):
  """
  Update values from specific columns

  Args:
      df (DataFrame): DataFrame on which to perform the modifications

  Returns:
      DataFrame: Update DataFrame
  """

  logging.info(f"Updated fields value")

  try:
    # Lowercase on all typeOfBuilding
    df["typeOfBuilding"] = df["typeOfBuilding"].str.lower()

    # Lowercase on all typeOfSearch & Update "vente" to "achat"
    df["typeOfSearch"] = df["typeOfSearch"].str.lower()
    df["typeOfSearch"] = np.where(
        df["typeOfSearch"] == "vente", "achat", df["typeOfSearch"])

    return df
  except Exception:
    logging.error("... Failed")
    raise


def validation_int(df, col, upper_bound, lower_bound):

  """
  Drop rows when col is not within [upper_bound, lower_bound]

  Args:
      df (DataFrame): DataFrame on which to perform the modifications
      col (str): Column name
      upper_bound (int): Upper bound
      lower_bound (int): Lower bound
  """

  logging.info(f"Validating {lower_bound} < {col} < {upper_bound}")

  try:
    df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]

    return df
  except KeyError:
    logging.error(f"... Failed >> KeyError {col}")
    raise
  except Exception:
    logging.error("... Failed")
    raise


def groupby(df):
  """
  Group by _idMutation, summing surface and nbRoom together and diplaying other field as such

  Args:
      df (DataFrame): DataFrame on which to perform the groupby

  Returns:
      DataFrame: Update DataFrame
  """

  logging.info(f"Grouping rows by id")

  try:
    df = df.groupby("_idMutation", as_index=True).agg({
        "createdAt": "first", "typeOfSearch": "first", "price": "first", "streetNumber": "first", "houseNumber": "first", "streetName": "first",
        "postalCode": "first", "city": "first", "departement": "first", "typeOfBuilding": "first", "surface": "sum", "nbRoom": "sum", "longitude": "first", "latitude": "first"
    })

    return df
  except Exception:
    logging.error("... Failed")
    raise


def save_df(df, args):
  """
  Save df as csv file with year in filename

  Args:
      df (DataFrame): DataFrame to save
      args (dict): Argument passed, includes year and save
  """

  if args["save"] is not None:
    logging.info(f"Saving DataFrame")

    try:
      df.to_csv(f"data/dvf_{args['year']}_updated.csv")
    except Exception as e:
      print(e)
      logging.error("... Failed")
      raise


def transform(args):
  try:

    # Init bar
    bar = ShadyBar(
        "Processing Data".ljust(25),
        max=6,
        width=50)
    
    # Transform
    df = csv_to_df(args["year"])
    bar.next()
    df = drop_na(df)
    bar.next()
    df = update_fields_values(df)
    bar.next()
    df = validation_int(df, "price", 1999999, 4999)
    bar.next()
    df = validation_int(df, "surface", 1001, 9)
    bar.next()
    df = groupby(df)
    bar.next()
    
    # Save df as csv
    save_df(df, args)

    bar.finish()

  except Exception:
      raise
    