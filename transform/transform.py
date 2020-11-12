# IMPORTS
import logging
import pandas as pd
import numpy as np
import json


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
                  "postalCode", "city", "state", "typeOfBuilding", "surface", "nbRoom", "longitude", "latitude"]

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
    # typeOfBuilding / streetName / city
    df["typeOfBuilding"] = df["typeOfBuilding"].str.lower()
    df["streetName"] = df["streetName"].str.lower()
    df["city"] = df["city"].str.lower()

    # typeOfSearch: Lowercase & Update "vente" to "achat"
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
        "postalCode": "first", "city": "first", "state": "first", "typeOfBuilding": "first", "surface": "sum", "nbRoom": "sum", "longitude": "first", "latitude": "first"
    })

    return df
  except Exception:
    logging.error("... Failed")
    raise


def save_df_to_json(df, args):

  if args["save"] is not None:
    logging.info(f"Saving DataFrame as JSON file")

    try:
      df.to_json(f"data/dvf_{args['year']}_updated.json", orient="records", date_format="iso")
      print("DataFrame saved to JSON file") if args["verbose"] else None
    except Exception as e:
      print(e)
      logging.error("... Failed")
      raise


def update_dates(json_file, args):
    data = []

    logging.info(f"Updating date format in JSON file")

    try:
      # Open json file
      with open(f"data/{json_file}.json", "r") as f:
          data = json.load(f)
      
      for d in data:
          d["createdAt"] = {"$date": d["createdAt"]}

      # Write to json file
      with open(f"data/{json_file}.json", 'w') as f:
          json.dump(data, f, indent=4)
      
      print("Dates modified in JSON file") if args["verbose"] else None
    except Exception as e:
      print(e)
      logging.error("... Failed")
      raise

def transform(args):
  try:
    df = csv_to_df(args["year"])
    print("CSV loaded to DataFrame") if args["verbose"] else None
    df = drop_na(df)
    print("NaN values dropped") if args["verbose"] else None
    df = update_fields_values(df)
    df = validation_int(df, "price", 1999999, 4999)
    print("Price validated") if args["verbose"] else None
    df = validation_int(df, "surface", 1001, 9)
    print("Surface validated") if args["verbose"] else None
    df = groupby(df)
    print("Rows grouped per id") if args["verbose"] else None
    
    # Save df
    save_df_to_json(df, args)

    # Update dates
    update_dates(f"dvf_{args['year']}_updated", args)


  except Exception:
      raise
    