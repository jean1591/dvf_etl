#!/bin/bash

# Retrieve year passed in args
while getopts y: option
do
  case "${option}"
  in
  y) year=${OPTARG};;
  esac
done

echo "Extracting dvf_$year.csv.gz into dvf_$year.csv"
gzip -d data/dvf_$year.csv.gz