#!/bin/bash

# Retrieve year passed in args
while getopts y: option
do
  case ${option}
  in
  y) year=${OPTARG};;
  esac
done

echo "Loading dvf_"$year"_updated.csv into mongoDB"
mongoimport --db perso --collection DVF --drop --type csv --file data/dvf_"$year"_updated.csv --headerline