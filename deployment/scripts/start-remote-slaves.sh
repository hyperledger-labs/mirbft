#!/bin/bash

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

exp_data_dir=$1
tag=$2
n=$3
master_ip=$4
shift 4

# Count how many slaves need to be skipped in the input
skip=0
while [ -n "$1" ] && [ "$1" = "skip" ] && [ $n -gt 0 ]; do
  if [ "$3" = "$tag" ]; then
    s=$2
    skip=$((skip + s))
  fi
  shift 3
done

# For each slave in the slaves file
while [ -n "$1" ] && [ $n -gt 0 ]; do

  # Read arguments
  instance_id=$1
  public_slave_ip=$2
  private_slave_ip=$3
  slave_tag=$4
  slave_region=$5
  shift 5

  if [ "$slave_tag" = "$tag" ] && [ $skip -gt 0 ]; then
    skip=$((skip - 1))
  elif [ "$slave_tag" = "$tag" ]; then
    echo "Deploying slave at public IP $public_slave_ip ($instance_id) tagged $slave_tag"

    scripts/start-slave.sh $slave_tag $master_ip $public_slave_ip $private_slave_ip > "$exp_data_dir/ssh-$slave_tag-$public_slave_ip.log" 2>&1 &

    # Introduced because if too many slaves are started at the same time,
    # The system cannot handle opening that many SSH connections in parallel.
    sleep 0.1

    # Decrement counter of slaves to deploy
    n=$((n - 1))

  fi
done
wait
