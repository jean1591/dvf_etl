# IMPORTS
import subprocess
import logging

def load(args):
  try:
    # Init command
    cmd = ["mongoimport", "--db", args['db'], "--collection", args['collection'], "--type", "csv", "--file", f"data/dvf_{args['year']}_updated.csv", "--headerline"]

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
    