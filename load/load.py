# IMPORTS
import subprocess
import logging
from os import listdir

def delete_file(filename):
  """
  Delete filename file in data directory

  Args:
      filename (str): Name of the file to be deleted
  """
  try:
    if filename in listdir("data/"):
      subprocess.run(["rm", f"data/{filename}"], check=True)
  except Exception:
    logging.error("... Failed")
    raise


def load(args):
  # Load CSV file
  try:
    logging.info(f"Loading dvf_{args['year']}_updated.json to DB")
    # Init command
    cmd = ["mongoimport", "--db", args['db'], "--collection", args['collection'], "--type", "json", "--file", f"data/dvf_{args['year']}_updated.json", "--jsonArray"]

    # Drop collection if specified
    cmd.append("--drop") if args["remove"] else None

    # Hide message is specified
    cmd.append("--quiet") if not args["verbose"] else None

    # Run command
    subprocess.run(cmd, check=True)
  except subprocess.CalledProcessError:
    logging.error("... Failed >> CalledProcessError")
    raise
  except Exception:
    logging.error("... Failed")
    raise

  # Delete files
  try:
    logging.info(f"Deleting dvf_{args['year']}_updated.json & dvf_{args['year']}.csv")
    delete_file(f"dvf_{args['year']}_updated.json")
    delete_file(f"dvf_{args['year']}.csv")
  except Exception:
    logging.error("... Failed")
    raise
    