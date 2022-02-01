#!/bin/bash

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

threads=32
analyze=false
analyzie_params=

# Override number of parallel threads
if [ "$1" = "-p" ]; then
  threads=$2
  shift 2
fi

exp_dir=$1
shift 1

if [ "$1" = "analyze" ]; then
  analyze=true
  shift
  analysis_params=$@
fi

function find_exp_ids() {
  # The regular expression is assuming a certain format of the archive file names ($remote_log_archives).
  # Adapt this function when that changes.

  # ls -1 $exp_dir/raw-results/$remote_log_archives \
  # | grep -P '(?<=experiment-output-)[0-9]+(?=-slave)' | uniq

  # This variant of the commented command avoids using Perl regex with lookaround not supported by grep on Mac.
  ls -1 $exp_dir/raw-results/$remote_log_archives \
  | grep -Eo 'experiment-output-[0-9]+-slave' | grep -Eo '[0-9]+' | uniq
}

function try_extract() {
  # The regular expression is assuming a certain format of the archive file names ($remote_log_archives).
  # Adapt this function when that changes.

  if [ -d "$exp_dir/experiment-output/$1" ]; then
    echo "$1: skip"
    return
  fi

  archives=$(ls -1 $exp_dir/raw-results/experiment-output-$1-slave-*.tar.gz 2>/dev/null | wc -l)
  if [ $archives -eq 0 ]; then
    echo "$1: nodata"
    return
  fi

  scripts/analyze/untar.sh "$exp_dir" $exp_dir/raw-results/experiment-output-$1-slave-*.tar.gz > /dev/null
  failures=$(ls -1 $exp_dir/experiment-output/$1/slave-*/FAILED 2>/dev/null | wc -l)

  if [ $failures -gt 0 ]; then
    echo "$1: FAILED"
    rm -r "${exp_dir:?}/experiment-output/${1:?}"
  else
    if $analyze; then
      echo "$1: analyzing"
      scripts/analyze/analyze.sh $analysis_params "$exp_dir/experiment-output/$1"
    else
      echo "$1: OK"
    fi
  fi
}


t=0
for exp_id in $(find_exp_ids); do

  # If number of threads has not yet been reached,
  # launch new extraction in the background.
  if [ $t -lt $threads ]; then
    try_extract $exp_id &
    t=$((t + 1))
  fi

  # Wait for all threads to finish when the max number has been reached.
  if [ $t -eq $threads ]; then
    wait
    t=0
  fi

done

# Wait for remaining threads
wait
