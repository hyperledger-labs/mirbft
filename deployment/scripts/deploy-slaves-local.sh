#!/bin/bash

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

# Get root directory of the deployment data
exp_data_dir=$1
shift

# For each tuple given on the command line
while [ -n "$1" ]; do

  # Read arguments
  trigger=$1
  n=$2
  tag=$3
  machine_template=$4
  shift 4

  echo "Deploy params: $trigger, $n, $tag, $machine_template"

  # Wait for trigger. We interpret the master status (a number)
  # reaching (or exceeding) the value of $trigger as a trigger.
  master_status=$(cat $exp_data_dir/$local_master_status_file)
  while [[ $((10#$trigger)) -ge 0 ]] && [[ ! ( "$master_status" =~ ^[0-9]+$ ) || ( $((10#$master_status)) -lt $((10#$trigger)) ) ]]; do
    # Note the $((10#$trigger)) operand. This tells bash to interpret $trigger as a decimal number.
    # Otherwise, if $trigger starts with '0' (which it sometimes does), $trigger is treated as an octal number.
    sleep $machine_status_poll_period
    master_status=$(cat $exp_data_dir/$local_master_status_file)
  done

  # Deploy slave nodes.
  echo "Changing directory to $exp_data_dir"
  initial_directory=$(pwd)
  cd $exp_data_dir || exit 1
  echo "Starting local slaves: $n $tag"
  for i in $(seq 1 $n); do
    echo discoveryslave $tag $local_public_ip:$master_port $local_public_ip $local_private_ip
    discoveryslave $tag $local_public_ip:$master_port $local_public_ip $local_private_ip > slave-$i.log 2>&1 &
  done
  echo "Changing directory back to $initial_directory"
  cd $initial_directory || exit 1

done
wait

echo "Local slave deployment finished."
