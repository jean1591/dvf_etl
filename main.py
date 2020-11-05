# IMPORTS
import argparse
import logging

# FILES
from extract import extract
from transform import transform
from load import load


def get_args():
    """
    Retrieve args

    Returns:
        dict: Args passed
    """
    parser = argparse.ArgumentParser()

    # Retrieve args
    parser.add_argument("-y", "--year", help="Year to Fetch")
    parser.add_argument("-d", "--db", help="DB")
    parser.add_argument("-c", "--collection", help="DB collection")
    parser.add_argument("-r", "--remove", action="store_true")
    parser.add_argument("-s", "--save", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    return {"year": args.year, "db": args.db, "collection": args.collection, "remove": args.remove, "save": args.save, "verbose": args.verbose}


def clear_log_file():
  with open("activity.log", "w"):
    pass


# INIT LOG FILE
logging.basicConfig(filename="activity.log", format="%(asctime)s::%(levelname)s::%(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")


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
    logging.error("ETL >> Failed")
    return

  if args["db"] is None:
    logging.error("DB not specified")
    logging.error("ETL >> Failed")
    return

  if args["collection"] is None:
    logging.error("Collection not specified")
    logging.error("ETL >> Failed")
    return
  

  print(f">> PERFORMING ETL ON DVF_{args['year']} <<") if args["verbose"] else None
  

  # >> EXTRACT
  logging.info("Extract >> Start")
  print("\n>> EXTRACT <<") if args["verbose"] else None
  try:
    extract(args)
    logging.info("Extract >> End")
  except Exception:
    logging.error("Extract >> Failed")
    logging.error("ETL >> Failed")
    return


  # >> TRANSFORM
  logging.info("Transform >> Start")
  print("\n>> TRANSFORM <<") if args["verbose"] else None
  try:
    transform(args)
    logging.info("Transform >> End")
  except Exception:
    logging.error("Transform >> Failed")
    logging.error("ETL >> Failed")
    return


  # >> LOAD
  logging.info("Load >> Start")
  print("\n>> LOAD <<") if args["verbose"] else None
  try:
    load(args)
    logging.info("Load >> End")
  except Exception:
    logging.error("Load >> Failed")
    logging.error("ETL >> Failed")
    return


  logging.info("ETL >> End")

clear_log_file()
main()