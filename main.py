# IMPORTS
from os import system
import argparse
import logging


def get_args():
    """
    Retrieve args

    Returns:
        dict: Args passed
    """
    parser = argparse.ArgumentParser()

    # Retrieve args
    parser.add_argument("-y", "--year", help="Year to Fetch")
    args = parser.parse_args()

    return {"year": args.year}


# Get args
args = get_args()
logging.basicConfig(filename="activity.log", format="%(levelname)s:%(message)s", level=logging.INFO)


def clear_log_file():
  with open("activity.log", "w"):
    pass


def main():
  """
  Script to perform ETL on a DVF file
  Source: https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/
  """
  logging.info("ETL >> Start")
  
  # EXTRACT
  logging.info("Extract >> Start")
  try:
    system(f"python3 1_extract/download_extract.py -y {args['year']}")
    logging.info("Extract >> End")
  except Exception as e:
    logging.error("Extract >> Failed")

  # TRANSFORM

  # LOAD


  logging.info("ETL >> End")

clear_log_file()
main()