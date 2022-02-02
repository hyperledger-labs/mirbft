#!/bin/bash
#
# usage: deploy-slaves-cloud.sh master_ip [trigger0 n0 label0 [trigger1 n1 label1 template1 [trigger2 n2 label2 template2 [...]]]]
#
# Deploys cloud machines based on master status.
# For each tuple (trigger, n, label) given as arguments on a command line, deploys n slave nodes with the given label
# when the master status is trigger or (numerically) higher.
# The input tuples are processed in the given order, so a later tuple is only applied when all previous tuples have
# received their triggers.
# The master must be up and running when this script starts.

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

# Read first two arguments (the rest is the deploy schedule)
master_ip=$1
exp_data_dir=$2
shift 2

# For each tuple given on the command line
while [ -n "$1" ]; do

  # Read arguments
  trigger=$1
  n=$2
  tag=$3
  machine_template=$4
  shift 4

  # Wait for trigger. We interpret the master status (a number)
  # reaching (or exceeding) the value of $trigger as a trigger.
  master_status=$(scripts/remote-machine-status.sh "$master_ip")
  while [[ $((10#$trigger)) -ge 0 ]] && [[ ! ( "$master_status" =~ ^[0-9]+$ ) || ( $((10#$master_status)) -lt $((10#$trigger)) ) ]]; do
    # Note the $((10#$trigger)) operand. This tells bash to interpret $trigger as a decimal number.
    # Otherwise, if $trigger starts with '0' (which it sometimes does), $trigger is treated as an octal number.
    sleep $machine_status_poll_period
    master_status=$(scripts/remote-machine-status.sh "$master_ip")
  done

  # Deploy slave nodes.
  echo "Deploying slaves: $n $tag on $machine_template"
  scripts/create-tagged-slave-instances.sh "$exp_data_dir" "$tag" "$machine_template" "$n" "$master_ip" &

done
wait

echo "Slave deployment on cloud finished. Copying instance info to: $default_instance_info."
cp "$exp_data_dir/$instance_info_file_name" "$default_instance_info"
