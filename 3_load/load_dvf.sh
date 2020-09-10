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

echo "Loading dvf_"$year"_updated.csv into "$db"."$collection
mongoimport --db $db --collection $collection --drop --type csv --file data/dvf_"$year"_updated.csv --headerline