# IMPORTS
import subprocess
import logging


def download(year):
    """
    Download data from https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{year}/full.csv.gz

    Args:
        year (int): Year to download
    """
    logging.info(f"Downloading dvf_{year}.csv.gz")
    try:
        subprocess.run(["wget", "-q", "--show-progress", "-O", f"data/dvf_{year}.csv.gz", f"https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{year}/full.csv.gz"], check=True)
    except subprocess.CalledProcessError:
        logging.error("... Failed >> CalledProcessError")
        raise
    except Exception:
        logging.error("... Failed")
        raise


def unzip(year):
    """
    Extract data from downloaded file

    Args:
        year (int): Year to extract
    """
    logging.info(f"Extracting dvf_{year}.csv.gz")
    try:
        subprocess.run(["gzip", "-d", "-f", f"data/dvf_{year}.csv.gz"], check=True)
    except subprocess.CalledProcessError:
        logging.error("... Failed >> CalledProcessError")
        raise
    except Exception:
        logging.error("... Failed")
        raise


def extract(args):
    try:
        # Download data
        download(args["year"])
        # Extract data
        unzip(args["year"])
    except subprocess.CalledProcessError:
        raise
    except Exception:
        raise