#!/bin/bash
#
# usage: re-scheduled-deploy.sh instance_info_file master_ip [trigger0 n0 label0 [trigger1 n1 label1 [trigger2 n2 label2 [...]]]]
#
# Deploys cloud machines based on master status.
# Uses slave machine addresses provided in instance_info_file instead of starting new ones.
# For each tuple (trigger, n, label) given as arguments on a command line, deploys n slave nodes with the given label
# when the master status is trigger or (numerically) higher.
# The input tuples are processed in the given order, so a later tuple is only applied when all previous tuples have
# received their triggers.
# The master must be up and running when this script starts.

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

exp_data_dir=$1
instance_info_file=$2
master_ip=$3
shift 3

# After deploying slaves, their numbers and tags will be appended here.
# Each new invocation of remote-deloy-slaves.sh will receive this list as the lat arguments and skip correspondingly
# many entries in the input.
skip="skip 0 master" # needs to be initialized with a dummy value for the parameter count to be right

# For each tuple given on the command line
while [ -n "$1" ]; do

  # Read arguments
  trigger=$1
  n=$2
  tag=$3
  shift 4 # shifting by one more, because the machine template file (also present in the deploy schedule is ignored).

  # Wait for trigger
  master_status=$(scripts/remote-machine-status.sh $master_ip)

  while [[ $((10#$trigger)) -ge 0 ]] && [[ ! ( "$master_status" =~ ^[0-9]+$ ) || ( $((10#$master_status)) -lt $((10#$trigger)) ) ]]; do
    # Note the $((10#$trigger)) operand. This tells bash to interpret $trigger as a decimal number.
    # Otherwise, if $trigger starts with '0' (which it sometimes does), $trigger is treated as an octal number.
    sleep $machine_status_poll_period
    master_status=$(scripts/remote-machine-status.sh $master_ip)
  done

  # Deploy slave nodes.
  echo "Deploying slaves: $n $tag"
  scripts/start-remote-slaves.sh "$exp_data_dir" "$tag" $n "$master_ip" $skip $(cat $instance_info_file) &

  skip="$skip skip $n $tag"
done

echo "All slaves started. waiting for them to finish."
wait
echo "Remote slave deployment finished."
