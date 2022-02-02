# This script is not to be ran on its own.
# It is the only included as the common part of the different kinds of deployments,
# Initializing the experiment, reading command-line arguments and stting some variables.
# It must be sourced, NOT ran even inside those scripts.
# It consumes up to 4 command line parameters and sets the following variables:
# - configuration_generator_script
# - depl_type
# - exp_data_dir
# - new_experiment
# - exp_id_offset
# - deployment_file
# - deploy_schedule
# - cancel_instances
# Some of them are used only internally some of them are used by this including script.

if [ "$1" = "-c" ]; then
  cancel_instances=true
  shift
else
  cancel_instances=false
fi

if [ -n "$1" ] && [ "$1" = "local" ]; then
  depl_type="$1"
  shift
elif [ -n "$1" ] && [ "$1" = "cloud" ]; then
  depl_type="$1"
  shift
elif [ -n "$1" ] && [ "$1" = "remote" ]; then
  depl_type="$1"
  instance_info_file="$2"
  shift 2
else
  depl_type="local" # Default deployment type
fi

# Get experiment data directory to work with, or create a new one.
if [ "$1" = "new" ]; then
  exp_data_dir=$(scripts/new-experiment-state.sh $depl_type)
  new_experiment=true
else
  exp_data_dir=$1
  new_experiment=false
fi
shift
echo "Using experiment data directory: $exp_data_dir"

# If we are creating a new set of experiments, generate the necessary config and deploy files.
if $new_experiment; then

  # Get config generator script to use.
  configuration_generator_script="$1"
  shift 1

  # Get the experiment id offset.
  # The experiments generated will start with this ID.
  if [ -n "$1" ]; then
    exp_id_offset=$1
    shift
  else
    exp_id_offset=0
  fi

  # Generate deployment file and configuration files
  "$configuration_generator_script" "$exp_data_dir" $exp_id_offset || exit 1

  # Save a copy of the configuration generator for easier reproducibility
  cp "$configuration_generator_script" "$exp_data_dir"
fi

# Parse deployment file, generating the master command file and a deployment schedule
deployment_file="$exp_data_dir/$dpl_filename"
echo "Using deployment file: $deployment_file"

deploy_schedule=$(python3 scripts/generate-master-commands.py $depl_type "$deployment_file" "$exp_data_dir/$local_master_command_template_file" "$exp_data_dir")
if [ $? -ne 0 ]; then
  >&2 echo "remote-deploy.sh: failed processing deployment file: $deployment_file"
  exit 2
fi
