# Check whether private key file exists
if ! [ -r "$private_key_file" ]; then
  >&2 echo "$0: cannot read private key file: $private_key_file"
  exit 1
fi

# Check whether machine creation template file exists
if ! [ -r "$master_machine" ]; then
  >&2 echo "$0: cannot read machine creation template file: $master_machine"
  exit 1
fi

# Instantiate user script for master server.
export ic_master_port=$master_port
export ic_private_ssh_key=$(cat $private_key_file)
envsubst '$ic_master_port $ic_private_ssh_key' < "$user_script_template_master" > "$exp_data_dir/user-script-master.sh"

# Launch master machine.
# The parameters (except for the first one) to new-cloud-instance.sh are directly forwarded to ibmcloud sl vs create
# with one exception: the --wait timeout parameter is added and handled specially inside ic-haunch-instance.sh
# The instance is created with the forwarded parameters and then a separate call to ibmcloud sl vs ready --wait ...
# waits for the instance to be ready. This is necessary due to a buggy behavior of ibmcloud sl vs created
# when the --wait option is used. Avoid using the --wait option.
echo "Deploying master."
master_info=$(scripts/new-cloud-instance.sh 1 \
                                     master \
                                     "$master_machine" \
                                     "$exp_data_dir/user-script-master.sh")
echo "$master_info" > $exp_data_dir/$instance_info_file_name
master_ip=$(echo "$master_info" | awk '{print $2}')
echo "Master deployed: $master_ip"

# Start slave deployment process in the background
echo "Starting scheduled deployment."
scripts/deploy-slaves-cloud.sh $master_ip "$exp_data_dir" $deploy_schedule & # No quotes around $deploy_schedule!

# Periodically check master status and wait until it is running
master_status=$(scripts/remote-machine-status.sh $master_ip)
echo "Master status: $master_status"
while ! [[ "$master_status" = "RUNNING" ]]; do
  # Sleep a bit and obtain new status.
  sleep $machine_status_poll_period
  master_status=$(scripts/remote-machine-status.sh $master_ip)
  echo "Master status: $master_status"
done

# Start the master server on the newly deployed machine.
scripts/start-master.sh "$exp_data_dir" "$master_ip" &

# Start result fetching in the background.
scripts/fetch-results.sh $master_ip "$exp_data_dir" > "$exp_data_dir/$local_result_fetching_log" 2>&1 &

echo "Waiting for deployment process and result fetching to finish."
echo "For progress on experiment result fetching, see $exp_data_dir/$local_result_fetching_log."
wait

# Cancel cloud machines if configured to do so.
if $cancel_instances; then
  scripts/cancel-cloud-instances.sh "$exp_data_dir/$instance_info_file_name"
else
  echo -e "Do not forget to cancel the used virtual servers using\n\n    scripts/cancel-cloud-instances.sh $exp_data_dir/$instance_info_file_name \n"
fi
