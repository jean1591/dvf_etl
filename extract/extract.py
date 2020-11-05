# IMPORTS
import subprocess
import logging


def download(args):
    """
    Download data from https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{year}/full.csv.gz

    Args:
        year (int): Year to download
    """
    logging.info(f"Downloading dvf_{args['year']}.csv.gz")
    print(f"Downloading https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{args['year']}/full.csv.gz") if args["verbose"] else None
    try:
        # Init command
        cmd = ["wget", "-q", "-O", f"data/dvf_{args['year']}.csv.gz", f"https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{args['year']}/full.csv.gz"]

        # Drop collection if specified
        cmd.append("--show-progress") if args["verbose"] else None

        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        logging.error("... Failed >> CalledProcessError")
        raise
    except Exception:
        logging.error("... Failed")
        raise


def unzip(args):
    """
    Extract data from downloaded file

    Args:
        year (int): Year to extract
    """
    logging.info(f"Extracting dvf_{args['year']}.csv.gz")
    print(f"Extracting dvf_{args['year']}.csv.gz into dvf_{args['year']}.csv") if args["verbose"] else None

    try:
        subprocess.run(["gzip", "-d", "-f", f"data/dvf_{args['year']}.csv.gz"], check=True)
    except subprocess.CalledProcessError:
        logging.error("... Failed >> CalledProcessError")
        raise
    except Exception:
        logging.error("... Failed")
        raise


def extract(args):
    try:
        # Download data
        download(args)
        # Extract data
        unzip(args)
    except subprocess.CalledProcessError:
        raise
    except Exception:
        raise