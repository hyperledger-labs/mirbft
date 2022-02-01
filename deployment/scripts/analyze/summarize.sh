#!/bin/bash

params_file=$1
root_dir=$2
shift 2

# Ids of all experiments in the parameters file.
all_ids=$(cat $params_file | awk -F ',' '{print $1}')

# All fields found in the result data.
# A field 'f' is considered to be present in the result data if the file 'f.val'
# Is present in at least one result directory.
fields=$(for exp_id in $all_ids; do
  for file in $(ls -1 $root_dir/$exp_id/*.val 2>/dev/null); do
    basename ${file%.*}
  done
done | sort | uniq)

# Add extra fields from command line
extra_fields=""
extra_vals=""
while [ -n "$1" ]; do
  extra_fields="${extra_fields},${1}"
  extra_vals="${extra_vals},${2}"
  shift 2
done

# The first line is the header line containing all the field names.
# This will be set to false after the first iteration.
header=true

# For each line in the parameters file.
while read -r line; do

  # Parse out the ID of the experiment.
  exp_id=$(echo $line | awk -F ',' '{print $1}')

  # Append value for each field, producing a line of the new CSV file.
  for field in $fields; do

    # First line is special, don't append a value, but the field name.
    if $header; then
      line="$line,$field"
    # For all other lines, append the field value value.
    else
      line="$line,$(cat $root_dir/$exp_id/$field.val)"
    fi
  done

  # Append extra fields to first line
  if $header; then
    echo "${line}${extra_fields}"
  # Append extra values to all other lines
  else
    echo "${line}${extra_vals}"
  fi

  header=false

done < "$params_file"