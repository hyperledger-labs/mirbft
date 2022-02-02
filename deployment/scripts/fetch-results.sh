#!/bin/bash

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

master_ip=$1
exp_dir=$2
raw_results=$exp_dir/raw-results
shift 2

# Wait until master server is ready.
echo "Waiting for master server."
while ! ssh $ssh_options -q -o "ConnectTimeout=10" "root@$master_ip" "cat $remote_ready_file > /dev/null"; do
  sleep $machine_status_poll_period
  echo "Master not ready. Retrying in $machine_status_poll_period seconds."
done

# Download code and binaries compiled at the master (for later analysis)
mkdir -p $exp_dir/gopath/bin
mkdir -p $exp_dir/gopath/src/$downloaded_code_dir
rsync --progress -rptz -e "ssh $ssh_options" root@$master_ip:$remote_gopath/bin/ordering* $exp_dir/gopath/bin/
rsync --progress -rptz -e "ssh $ssh_options" root@$master_ip:$remote_code_dir/* $exp_dir/gopath/src/$downloaded_code_dir/

# Create directory for raw experiment results
mkdir -p $raw_results || exit 1

# Check master status and download experiment output (probably none to download yet).
master_status=$(scripts/remote-machine-status.sh $master_ip)
if [[ "$master_status" =~ ^[0-9]+$ || "$master_status" = "DONE" || "$master_status" = "ANALYZED" ]]; then
  echo "Downloading log data"
  rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:$remote_exp_dir/raw-results/$remote_log_archives" $raw_results
fi

# Periodically check master status.
echo "Master status: $master_status"
while [[ "$master_status" != "DONE" && "$master_status" != "ANALYZED" ]]; do

  # Sleep a bit and obtain new status.
  sleep $machine_status_poll_period
  old_master_status=$master_status
  master_status=$(scripts/remote-machine-status.sh $master_ip)

  # Whenever master status changes, print it.
  if [ "$master_status" != "$old_master_status" ]; then
    echo "Master status: $master_status"

    # In addition, if status is a number or "DONE" or "ANALYZED, download additional experiment output.
    if [[ "$master_status" =~ ^[0-9]+$ ]] || [[ "$master_status" = "DONE" ]] || [[ "$master_status" != "ANALYZED" ]]; then
      rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:$remote_exp_dir/raw-results/$remote_log_archives" "$raw_results"
    fi
  fi

done

# Download last part of the output
# (In case the last experiment did not even start downloading when status changed to DONE.)
rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:$remote_exp_dir/raw-results/$remote_log_archives" "$raw_results"

echo "Downloading scripts, queries, and master log."
rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:scripts" "$exp_dir/"
rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:queries" "$exp_dir/"
rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:$remote_master_log" "$exp_dir/$local_master_log"

# Wait until the results at the master have been analyzed.
echo "Waiting for result analysis to finish at the master."
while [[ "$master_status" != "ANALYZED" ]]; do

  # Sleep a bit and obtain new status.
  sleep $machine_status_poll_period
  master_status=$(scripts/remote-machine-status.sh $master_ip)

done

echo "Downloading analyzed results."
rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:$remote_exp_dir/experiment-output" "$exp_dir/"
rsync --progress -rtz -e "ssh $ssh_options" "root@$master_ip:$remote_exp_dir/continuous-analysis.log" "$exp_dir/"

## This is a rather dirty hack for analyzing the profiles.
## remote-gopath should be the target of a symlink located at the actual gopath of the remote machines.
## During the analysis, we further point to the directory containing the actual codes and binaries downloaded
## from the remote machine.
#
## This does not work if the directory is mounted from the host machine on a VirtualBox VM.
## ln -s $exp_data_dir/gopath remote-gopath
## As a workaround, we temporarily copy all the files to remote-gopath
#mkdir remote-gopath
#cp -r $exp_dir/gopath/* remote-gopath
## Perform the analysis with the sources and binaries in the right place
#scripts/analyze/extract-successful.sh $exp_dir analyze $analysis_query_params -d
## Clean up
#rm -r remote-gopath

echo "Done fetching results."
