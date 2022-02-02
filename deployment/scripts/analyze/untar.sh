#!/bin/bash

output_dir=$1
shift

while [ -n "$1" ]; do
  echo "Unpacking file: $1"
  tar --directory "$output_dir" -xf "$1"
  shift
done
