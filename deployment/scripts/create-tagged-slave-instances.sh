#!/bin/bash

# usage: create-tagged-slave-instances.sh data_dir template_file tag num_slaves master_ip
#
# Outputs the IPs of the started machines.

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

exp_data_dir=$1
tag=$2
machine_template=$3
num_slaves=$4
master_ip=$5

# Check whether machine creation template file exists
if ! [ -r "$machine_template" ]; then
  >&2 echo "$0: cannot read machine creation template file: $machine_template"
  exit 1
fi

# Instantiate user script for slave.
export ic_master_address="$master_ip:$master_port"
export ic_tag=$tag
export ic_private_ssh_key=$(cat $private_key_file)
envsubst '$ic_master_address $ic_tag $ic_private_ssh_key' < "$user_script_template_slave" > "$exp_data_dir/user-script-slave-$tag.sh"

# Launch slave machines in parallel.
# The parameters (except for the first one) to new-cloud-instance.sh are directly forwarded to ibmcloud sl vs create
# with one exception: the --wait timeout parameter is added and handled specially inside ic-haunch-instance.sh
# The instance is created with the forwarded parameters and then a separate call to ibmcloud sl vs ready --wait ...
# waits for the instance to be ready. This is necessary due to a buggy behavior of ibmcloud sl vs create
# when the --wait option is used. Avoid using the --wait option.
slave_info=$(scripts/new-cloud-instance.sh "$num_slaves" \
                              "$tag" \
                              "$machine_template" \
                              "$exp_data_dir/user-script-slave-$tag.sh")
echo "$slave_info" >> $exp_data_dir/$instance_info_file_name
echo "Deployed $num_slaves slaves: $tag"
echo "$slave_info"

slave_public_ips=$(echo "$slave_info" | awk '{print $2}')
for slave_public_ip in $slave_public_ips; do
  slave_private_ip=$(echo "$slave_info" | awk "\$2 == \"$slave_public_ip\" {print \$3}")
  scripts/start-slave.sh $tag $master_ip $slave_public_ip $slave_private_ip > "$exp_data_dir/ssh-$tag-$slave_public_ip.log" 2>&1 &
done
wait