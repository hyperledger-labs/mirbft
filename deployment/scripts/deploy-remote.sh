# Obtain IP of the master node
master_ip=$(cat $instance_info_file | awk '$4 == "master" {print $2}')
if [ -z "$master_ip" ]; then
  >&2 echo "remote-deploy.sh: could not obtain master ip from instance info file: $instance_info_file"
  exit 1
fi
cp $instance_info_file $exp_data_dir/$instance_info_file_name
echo "Using instance info file: $instance_info_file"
echo "       Master IP address: $master_ip"


# Generate final master command file
export ssh_key_file=$remote_private_key_file
export own_public_ip=$master_ip
export master_port
export status_file=$remote_status_file
envsubst '$ssh_key_file $own_public_ip $master_port $status_file' < "$exp_data_dir/$local_master_command_template_file" > "$exp_data_dir/$local_master_command_file"
echo -e "\nwrite-file $status_file DONE" >> "$exp_data_dir/$local_master_command_file"

# Kill everything that is alive on the remote machines and prune old state
echo "Killing everything that is alive and pruning state on the remote machines (including SSH) and removing potential bandwidth limit."

for ip in $(cat $instance_info_file | awk '{print $2}'); do
  # the grep -v \$\$ prevents the script from killing itself
  ssh $ssh_options root@$ip "kill -9 \$(ps -ef | grep 'analyze-continuously' | grep -v \$\$ | awk '{print \$2}')" &
  sleep 0.1 # Opening too many SSH connections at once makes some of them fail (keeping many open is OK, however).
done
wait

echo -e "\nKilled continuous analysis scripts.\n"

for ip in $(cat $instance_info_file | awk '{print $2}'); do
  ssh $ssh_options root@$ip "
    tc qdisc del dev eth0 root tbf rate 1gbit burst 320kbit latency 400ms
    killall -9 discoverymaster discoveryslave orderingpeer orderingclient scp rsync
    rm -rf $remote_delete_files
    echo RUNNING > $remote_status_file
    kill -9 \$(ps -ef | grep 'sshd: root@notty' | awk '{print \$2}')
    echo -e '\n\n\nBERO\n\n\n'" &
  sleep 0.1 # Opening too many SSH connections at once makes some of them fail (keeping many open is OK, however).
done
wait

echo -e "\n Reset machine state.\n"

# Start the master server.
scripts/start-master.sh "$exp_data_dir" "$master_ip" &

# Start slaves according to schedule
scripts/deploy-slaves-remote.sh "$exp_data_dir" "$instance_info_file" "$master_ip" $deploy_schedule &

# Start result fetching in the background.
scripts/fetch-results.sh $master_ip "$exp_data_dir" > $exp_data_dir/$local_result_fetching_log 2>&1 &

echo "Waiting for deployment process and result fetching to finish."
echo "For progress on experiment result fetching, see $exp_data_dir/$local_result_fetching_log."
wait

# Cancel cloud machines if configured to do so.
if $cancel_instances; then
  scripts/cancel-cloud-instances.sh "$exp_data_dir/$instance_info_file_name"
else
  echo -e "Do not forget to cancel the used virtual servers using\n\n    scripts/cancel-cloud-instances.sh $exp_data_dir/$instance_info_file_name \n"
fi
