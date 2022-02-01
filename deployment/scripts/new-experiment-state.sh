#!/bin/bash

source scripts/global-vars.sh

# Type of experiment. "local" or "cloud"
type=$1
shift

mkdir -p "$deployment_data_root"

# Find next free directory number.
i=0
data_dir="$deployment_data_root/$type-0000"
while [ -d "$data_dir" ]; do
  i=$((i + 1))
  data_dir=$(printf "$deployment_data_root/$type-%04d" $i)
done

mkdir "$data_dir"
echo "$data_dir"
