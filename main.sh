#!/bin/bash

# Retrieve year passed in args
while getopts y: option
do
  case "${option}"
  in
  y) year=${OPTARG};;
  esac
done

echo ">> PERFORMING ETL ON DVF-$year <<"

echo
echo ">> EXTRACT"
sh 1_extract/download_dvf.sh -y $year
sh 1_extract/extract_dvf.sh -y $year

echo
echo ">> TRANSFORM"
python3 2_transform/transform_dvf.py -y $year

echo ">> EOF <<"