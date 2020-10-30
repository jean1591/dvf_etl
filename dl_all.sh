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

echo ">> PERFORMING ETL ON ALL DVF <<"

sh main.sh -y 2014 -d $db -c $collection
sh main.sh -y 2015 -d $db -c $collection
sh main.sh -y 2016 -d $db -c $collection
sh main.sh -y 2017 -d $db -c $collection
sh main.sh -y 2018 -d $db -c $collection
sh main.sh -y 2019 -d $db -c $collection

echo ">> ALL DVF HAVE BEEN DL <<"