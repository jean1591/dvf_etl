#!/bin/bash

# Retrieve year passed in args
while getopts y:d:c: option
do
  case "${option}"
  in
  y) year=${OPTARG};;
  d) db=${OPTARG};;
  c) collection=${OPTARG};;
  esac
done

echo ">> PERFORMING ETL ON DVF_$year <<"

echo
echo ">> EXTRACT"
sh 1_extract/download_dvf.sh -y $year
sh 1_extract/extract_dvf.sh -y $year

echo
echo ">> TRANSFORM"
python3 2_transform/transform_dvf.py -y $year

echo
echo ">> LOAD"
sh 3_load/load_dvf.sh -y $year -d $db -c $collection

echo
echo ">> EOF <<"