#!/bin/bash

exp_dir=$1
status_file=$2
script_dir=$3
query_dir=$4
peer_binary=$5
client_binary=$6
shift 6

if [ -n "$1" ]; then
  num_processors=$1
else
  num_processors=1
fi

source $script_dir/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

# This function is a modified version of 'try_extract()' from 'extract-successful.sh'.
function process_experiment() {
  # The regular expression is assuming a certain format of the archive file names ($remote_log_archives).
  # Adapt this function when that changes.

  local exp_id="$1"
  local archives
  local failures

  if [ -d "$exp_dir/experiment-output/$1" ]; then
    echo "$exp_id: skip"
    return
  fi

  archives=$(ls -1 $exp_dir/raw-results/experiment-output-$exp_id-slave-*.tar.gz 2>/dev/null | wc -l)
  if [ $archives -eq 0 ]; then
    echo "$exp_id: nodata"
    return
  fi

  $script_dir/analyze/untar.sh "$exp_dir" $exp_dir/raw-results/experiment-output-$exp_id-slave-*.tar.gz > /dev/null
  failures=$(ls -1 $exp_dir/experiment-output/$exp_id/slave-*/FAILED 2>/dev/null | wc -l)

  if [ $failures -gt 0 ]; then
    echo "$exp_id: FAILED"
    rm -r "${exp_dir:?}/experiment-output/${exp_id:?}"
  else
    echo "$1: analyzing"
    $script_dir/analyze/analyze.sh $analysis_query_params -b $peer_binary -c $client_binary -d "$exp_dir/experiment-output/$exp_id"
  fi
}

function result_processor() {
# Periodically check master status and analyze results after each experiment.
  local next_exp=$1
  local next_exp_id=$(printf "%0${exp_id_digits}d" $next_exp)
  local step=$2
  local stauts=$(cat $status_file 2>/dev/null)

  echo "Processing experiment results. Start: $next_exp Step: $step"

  # Stopping condition:
  # Experiments are DONE
  # AND
  # There are no more archive files to process (in case the experiments are DONE before this result processor sees the number of its own experiment)
  while [ "$status" != "DONE" ] || [ $(ls -1 $exp_dir/raw-results/experiment-output-$next_exp_id-slave-*.tar.gz 2>/dev/null | wc -l) -ne 0 ]; do

    # If status is greater or equal to the next experiment to analyze,
    # OR status is DONE and there are results to process
    # process results
    if [[ "$status" =~ ^[0-9]+$ && $((10#$status)) -ge $next_exp ]] \
    || [[ "$status" =  "DONE"   && $(ls -1 $exp_dir/raw-results/experiment-output-$next_exp_id-slave-*.tar.gz 2>/dev/null | wc -l) -ne 0 ]]; then
      process_experiment "$next_exp_id"
      next_exp=$((next_exp + $step))
      next_exp_id=$(printf "%0${exp_id_digits}d" $next_exp)
    fi

    # Sleep a bit and refresh status
    sleep $machine_status_poll_period
    status=$(cat $status_file 2>/dev/null)

  done

  echo "Finished processing results. First unprocessed: $next_exp"
}

# ========== Main script ==========

for i in $(seq 0 $((num_processors - 1))); do
  result_processor $i $num_processors &
done
wait

echo "Finished continuous result analysis."
echo "ANALYZED" > $status_file
