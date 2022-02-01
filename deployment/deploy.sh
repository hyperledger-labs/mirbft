#!/bin/bash

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

# The '-i' or '--init-only' flag makes the script exit after locally initializing the deployment, without running it.
if [ "$1" = "-i" ] || [ "$1" = "--init-only" ]; then
  init_only=true
  shift
else
  init_only=false
fi

# Initializes the deployment.
# This part of the script is separated, because it is reused by other kinds of deployments.
# It consumes multiple command line parameters and sets the following variables:
# - configuration_generator_script
# - depl_type
# - exp_data_dir
# - new_experiment
# - exp_id_offset
# - deployment_file
# - deploy_schedule
# - instance_info_file
# - cancel_instances
# Some of them are used only internally some of them are used by this including script.
source scripts/initialize-deployment.sh

# Exit if only initialization is required.
if $init_only; then
  echo "Init only. Experiment directory: $exp_data_dir"
  exit 0
fi

# Start the deployment
if [ "$depl_type" = "local" ]; then
  source scripts/deploy-local.sh
elif [ "$depl_type" = "cloud" ]; then
  source scripts/deploy-cloud.sh
elif [ "$depl_type" = "remote" ]; then
  source scripts/deploy-remote.sh
else
  >&2 echo "$0: unknown deployment type: $depl_type (allowed values: local, cloud, remote)"
fi

echo "Generating result summary."
scripts/analyze/summarize.sh $exp_data_dir/$csv_filename $exp_data_dir/experiment-output 2> /dev/null | tee $exp_data_dir/$result_summary_file

echo "Done. Experiment data directory: $exp_data_dir"
