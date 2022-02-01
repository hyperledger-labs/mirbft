#!/bin/bash

# Cancels all instances with the provided tags.
# __all__ is a special value matching all tags.
# Given an instance info file, cancels all instances listed in that file

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

function cancel_instance() {
  if ! echo "$except_list" | grep -q $1; then
    echo "Cancelling instance: $1"
    ibmcloud sl vs cancel "$1" --force
  else
    echo "Skipping excluded instance: $1"
  fi
}

function instance_ids_by_tag() {
  if [ "$1" = "__all__" ]; then
    ibmcloud sl vs list | tail -n +2 | awk '{print $1}'
  else
    ibmcloud sl vs list --tag "$1" | tail -n +2 | awk '{print $1}'
  fi
}

function instance_ids_from_file() {
  awk '{print $1}' < "$1"
}

function cancel_instances() {

  # Cancel each instance separately
  for id in "$@"; do
    cancel_instance "$id"
#    cancel_instance "$id" & # If too many instances are being canceled in parallel, the system hangs.
  done
  wait
}

# Cancel instances with all the tags given as parameters to this script
# or, if the argument is a file, instances listed in the first column.
while [ -n "$1" ]; do
  if [ "$1" = "-e" ] || [ "$1" = "--except" ]; then
    shift
    exclude=$(cat $1 | awk '{print $1}')
    except_list=$(echo -e "$except_list\n$exclude")
    echo "Exclude file: $1"
  elif [ -f "$1" ]; then
    echo "Cancelling instances listed in file: $1"
    instance_ids=$(instance_ids_from_file "$1")
  else
    echo "Cancelling instances with tag: $1"
    instance_ids=$(instance_ids_by_tag "$1")
  fi

  cancel_instances $instance_ids

  shift

done
