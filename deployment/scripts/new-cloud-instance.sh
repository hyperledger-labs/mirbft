#! /bin/bash

# TODO: Update this description.
# Launch a single slave machine.
# The parameters (except for the first two) to new-cloud-instance.sh are directly forwarded to ibmcloud sl vs create
# with one exception: the --wait timeout parameter is added and handled specially inside ic-haunch-instance.sh
# The instance is created with the forwarded parameters and then a separate call to ibmcloud sl vs ready --wait ...
# waits for the instance to be ready. This is necessary due to a buggy behavior of ibmcloud sl vs create
# when the --wait option is used. Avoid using the --wait option.
# The calls to ibmcloud sl vs ready sometimes fail (making ibmcloud sl vs create --wait sometimes fail too,
# asi it internally probably uses ready). This script retries the calls to ready if they fail.

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

# The first parameter says how many identical instances should be created.
num_instances=$1
tag=$2
machine_template_file=$3
user_script=$4
shift 4

# Fetches detailed information about the instance (id passed as argument)
# and saves it in a temporary file (to be later copied to the instance itself).
# Retries on failure until it succeeds.
function instance_detail() {
  # Get instance detail.
  # Suppressing the error output, as errors are treated by this script and the default output clutters the console.
  ibmcloud sl vs detail --output JSON "$1" > "ic-instance-detail-$1.json.tmp" 2>/dev/null
  exit_code=$?

  # Retry getting instance detail until command succeeds.
  while [ $exit_code -ne 0 ]; do
    >&2 echo "$0: Fetching detail of instance $1 failed (exit code $exit_code). Retrying after 10 seconds."
    sleep 10
    ibmcloud sl vs detail --output JSON "$id" > "ic-instance-detail-$1.json.tmp" 2>/dev/null
    exit_code=$?
  done
}

function wait_for_instance() {
  local instance_id=$1
  local public_ip=""
  local private_ip=""
  local exit_code

  # Fetch instance detail to obtain instance network addresses.
  instance_detail "$instance_id"
  public_ip=$(cat "ic-instance-detail-$instance_id.json.tmp" | jq -r '.primaryIpAddress')
  private_ip=$(cat "ic-instance-detail-$instance_id.json.tmp" | jq -r '.primaryBackendIpAddress')
  datacenter=$(cat "ic-instance-detail-$instance_id.json.tmp" | jq -r '.datacenter.name')
  # Keep fetching instance detail until it contains both the public and the private address.
  while ! [[ "$public_ip" =~ [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+ && "$private_ip" =~ [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+ ]]; do
    sleep 10
    instance_detail "$instance_id"
    public_ip=$(cat "ic-instance-detail-$instance_id.json.tmp" | jq -r '.primaryIpAddress')
    private_ip=$(cat "ic-instance-detail-$instance_id.json.tmp" | jq -r '.primaryBackendIpAddress')
    datacenter=$(cat "ic-instance-detail-$instance_id.json.tmp" | jq -r '.datacenter.name')
  done

  >&2 echo "Instance ID: $instance_id   Public IP: $public_ip   Private IP: $private_ip   Tag: $tag"

  # Upload instance tag to instance itself. This also serves to see if instance is up and running.
  ssh $ssh_options -o "LogLevel=QUIET" -o "ConnectTimeout=10" "root@$public_ip" "echo '$tag' > '$remote_instance_tag_file'" > /dev/null
  exit_code=$?
  # Retry executing dummy command over SSH until it succeeds
  while [ $exit_code -ne 0 ]; do
    >&2 echo "$0: Instance $instance_id with IP ${public_ip} not yet running. Retrying connection in 10 seconds."
    sleep 10
    ssh $ssh_options -o "LogLevel=QUIET" -o "ConnectTimeout=10" "root@$public_ip" "echo '$tag' > '$remote_instance_tag_file'" > /dev/null
    exit_code=$?
  done

  # Upload instance detail to the instance itself, for use by the guest OS.
  # Suppress standard output from SSH, as the only output from this script should be the instance's public IP (for use with further scripts).
  scp $ssh_options -o "LogLevel=QUIET" "ic-instance-detail-$instance_id.json.tmp" "root@$public_ip:$remote_instance_detail_file" > /dev/null
  exit_code=$?
  while [ $exit_code -ne 0 ]; do
    >&2 echo "$0: Failed to uplaod detail file to instance ${instance_id}. Retrying in 10 seconds."
    sleep 10
    scp $ssh_options -o "LogLevel=QUIET" "ic-instance-detail-$instance_id.json.tmp" "root@$public_ip:$remote_instance_detail_file" > /dev/null
    exit_code=$?
  done
  rm "ic-instance-detail-$instance_id.json.tmp"

  # Upload user script to the instance.
  # Suppress standard output from SSH, as the only output from this script should be the instance's public IP (for use with further scripts).
  scp $ssh_options -o "LogLevel=QUIET" "$user_script" "root@$public_ip:$remote_user_script_body" > /dev/null
  exit_code=$?
  while [ $exit_code -ne 0 ]; do
    >&2 echo "$0: Failed to uplaod user script to instance ${instance_id}. Retrying in 10 seconds."
    sleep 10
    scp $ssh_options -o "LogLevel=QUIET" "$user_script" "root@$public_ip:$remote_user_script_body" > /dev/null
    exit_code=$?
  done

  # Notify the the instance about the upload of the previous file.
  # Note that this needs to be done separately. If the instance directly checked for presence of the detail file,
  # it could (and did) happen that the instance proceeds with only a partially uploaded file.
  ssh $ssh_options -o "LogLevel=QUIET" "root@$public_ip" "touch $remote_user_script_uploaded" > /dev/null
  exit_code=$?
  while [ $exit_code -ne 0 ]; do
    >&2 echo "$0: Failed to confirm detail file upload to instance ${instance_id}. Retrying in 10 seconds."
    sleep 10
    ssh $ssh_options -o "LogLevel=QUIET" "root@$public_ip" "touch $remote_user_script_uploaded" > /dev/null
    exit_code=$?
  done

  # Print instance info (for use with further scripts).
  echo "$instance_id $public_ip $private_ip $tag $datacenter"

  # TODO: Remove this line, as technically it is not an error (and standard output is used by the calling script).
  >&2 echo "Instance $instance_id: $public_ip $private_ip ($tag) running."
}

# Creates a new virtual server instance and prints its ID.
# (The output is channeled to a bash variable when this function is called.)
# Arguments passed to this function are forwarded to ibmcloud sl vs create.
# Retries on failure until it succeeds.
function create_instances() {

  # Number of instances to create in this function call
  local n=$1

  # Prepare string to pass as machine creation parameters.
  # This tedious way is a workaround for the problem of ibmcloud sl vs create not supporting the creation of multiple
  # servers in one call.
  # Read user script
  bootstrap_script_data=$(cat $server_bootstrap_script)
  # Add user script as user data to machine template json object
  randomString=$(tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
  hostname=$(printf "$tag-%s" $randomString)
  machine_template=$(cat $machine_template_file | jq --compact-output --arg userData "$bootstrap_script_data" --arg hostname "$hostname" '. + {userData: [{value: $userData}]} + {hostname: $hostname}')
  # Copy machine template multiple times in a string to be passed to the api call
  creation_params="
  $machine_template"
  for i in $(seq 2 $n); do # Starting at 2, since the first machine has already been added at the previous line.

    # Generate a unique hostname each time
    randomString=$(tr -dc A-Za-z0-9 < /dev/urandom | head -c 8)
    hostname=$(printf "$tag-%s" $randomString)
    machine_template=$(cat $machine_template_file | jq --compact-output  --arg userData "$bootstrap_script_data" --arg hostname "$hostname" '. + {userData: [{value: $userData}]} + {hostname: $hostname}')

    creation_params="$creation_params, $machine_template"
  done

  # Create new virtual server instance
  create_output=$(ibmcloud sl call-api SoftLayer_Virtual_Guest createObjects --parameters "[[$creation_params]]")
  exit_code=$?
  instance_ids=$(echo "$create_output" | jq .[].id)
  if [[ (-n "$instance_ids") && ($exit_code -ne 0) ]]; then
    >&2 echo -e "\n\nWARNING! ibmcloud API returned exit status $exit_code but produced instance IDs:\n $instance_ids.\nNeed to cancel these instances manually!\n\n"
  fi

  # Retry creating virtual server instance until command succeeds.
  while [ $exit_code -ne 0 ]; do
    >&2 echo "$0: Creating virtual server instance failed (exit code $exit_code). Retrying after 10 seconds."
    sleep 10
    create_output=$(ibmcloud sl call-api SoftLayer_Virtual_Guest createObjects --parameters "[[$creation_params]]")
    exit_code=$?
    instance_ids=$(echo "$create_output" | jq .[].id)
    if [[ (-n "$instance_ids") && ($exit_code -ne 0) ]]; then
      >&2 echo -e "\n\nWARNING! ibmcloud API returned exit status $exit_code but produced instance IDs:\n $instance_ids.\nNeed to cancel these instances manually!\n\n"
    fi
  done

  echo $instance_ids
}

while [ $num_instances -gt 0 ]; do

  # Invoke virtual server creation in batches.
  # Since each instance of the virtual server is passed as a separate json object to the IBM Cloud CLI,
  # Creating too many servers at once may result in the argument list being too long for the shell to process.
  if [ $num_instances -ge $instance_creation_batch ]; then
    instance_ids=$(create_instances $instance_creation_batch)
    num_instances=$((num_instances - instance_creation_batch))
  else
    instance_ids=$(create_instances $num_instances)
    num_instances=0
  fi

  # Make sure instances are up and running and upload detail files to the machines.
  # Running this function in the background (and waiting for it after the main loop)
  # allows to start new batches of machines while waiting for the already started ones.
  for id in $instance_ids; do
    wait_for_instance $id &
    sleep 1
  done

done

wait
