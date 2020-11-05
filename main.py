# IMPORTS
import argparse
import logging

# FILES
from extract import extract
from transform import transform


def get_args():
    """
    Retrieve args

    Returns:
        dict: Args passed
    """
    parser = argparse.ArgumentParser()

    # Retrieve args
    parser.add_argument("-y", "--year", help="Year to Fetch")
    parser.add_argument("-s", "--save", action="store_true")
    args = parser.parse_args()

    return {"year": args.year, "save": args.save}


def clear_log_file():
  with open("activity.log", "w"):
    pass


# INIT LOG FILE
logging.basicConfig(filename="activity.log", format="%(asctime)s::%(levelname)-10s::%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")


def main():
  """
  Script to perform ETL on a DVF file
  Source: https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/
  """

  logging.info("ETL >> Start")


  # ARGS & CHECKS
  args = get_args()

  if args["year"] is None:
    logging.error("Year not specified")
    logging.error("ETL >> Failed >> End")
    return
  

  # >> EXTRACT
  logging.info("Extract >> Start")
  try:
    extract(args)
    logging.info("Extract >> End")
  except Exception:
    logging.error("Extract >> Failed")
    logging.error("ETL >> Failed")
    return


  # >> TRANSFORM
  logging.info("Transform >> Start")
  try:
    transform(args)
    logging.info("Transform >> End")
  except Exception:
    logging.error("Transform >> Failed")
    logging.error("ETL >> Failed")
    return


  # >> LOAD


  logging.info("ETL >> End")

clear_log_file()
main()