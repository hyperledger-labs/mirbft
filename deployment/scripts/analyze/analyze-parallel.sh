#!/bin/bash

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

threads=32

# Parameters to forward to the analyzing script
params=""

# Directories with experiments to analyze
experiments=""
while [ -n "$1" ]; do

  if [ "$1" = "-p" ]; then
    shift
    threads=$1
  elif [ "$1" = "-b" ]; then
    shift
    params="$params -b $1"
  elif [ "$1" = "-c" ]; then
    shift
    params="$params -c $1"
  elif [ "$1" = "-q" ]; then
    shift
    params="$params -q $1"
  elif [ "$1" = "-f" ] || [ "$1" = "--force" ]; then
    shift
    params="$params -f $1"
  else
    experiments="$experiments $1"
  fi

  shift
done

echo "Analyzing experiments using $threads threads."

t=0
for experiment in $experiments; do

  # If number of threads has not yet been reached,
  # launch new analysis in the background.
  if [ $t -lt $threads ]; then
    scripts/analyze/analyze.sh $params $experiment &
    t=$((t + 1))
  fi

  # Wait for all threads to finish when the max number has been reached.
  if [ $t -eq $threads ]; then
    wait
    t=0
  fi
done

# Wait for the remaining threads
wait

echo "Done analyzing experiments."
