#!/bin/bash

# Retrieve year passed in args
while getopts y: option
do
  case "${option}"
  in
  y) year=${OPTARG};;
  esac
done

echo "Downloading https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/$year/full.csv.gz"
wget -q --show-progress -O data/dvf_$year.csv.gz https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/$year/full.csv.gz