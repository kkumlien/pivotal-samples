#!/bin/bash

# Load the three small .tsv.gz files using COPY, on HAWQ master
# The files are TAB delimited, where NULL is just the empty string, ''

schema=retail_demo

for table in `ls *.gz  | perl -ne 's/^(\w+).+$/$1/;print;'`
do
  file="$table.tsv.gz"
  zcat $file | psql -c "COPY $schema.${table}_hawq FROM STDIN DELIMITER E'\t' NULL E'';"
done
