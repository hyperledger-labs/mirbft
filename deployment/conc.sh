#!/bin/bash
for x in {A..Z} {a..z}; do
  if [ -n "$1" ]; then
    if [ $x != "A" ]; then
      input="tail -n +2 $1"
    else
      input="cat $1"
    fi
    $input | while read -r line; do
      echo "$x,$line"
    done
    shift
  fi
done
