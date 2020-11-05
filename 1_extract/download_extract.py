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

def download(year):
    """
    Download data from https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{year}/full.csv.gz

    Args:
        year (int): Year to download
    """
    logging.info(f"Downloading dvf_{args['year']}.csv.gz")
    try:
        system(f"wget -q --show-progress -O data/dvf_{year}.csv.gz https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{year}/full.csv.gz")
        logging.info(f"... Done")
    except Exception as e:
        logging.error(f"... Failed")


def extract(year):
    """
    Extract data from downloaded file

    Args:
        year (int): Year to extract
    """
    logging.info(f"Extracting dvf_{args['year']}.csv.gz")
    try:
        system(f"gzip -d data/dvf_{year}.csv.gz")
        logging.info(f"... Done")
    except Exception as e:
        logging.error(f"... Failed")


# Download data
download(args['year'])

# Extract data
extract(args['year'])
