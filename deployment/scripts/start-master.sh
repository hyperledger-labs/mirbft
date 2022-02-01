#!/bin/bash

source scripts/global-vars.sh

# Kill all children of this script when exiting
trap "$trap_exit_command" EXIT

exp_data_dir=$1
master_ip=$2

# Generate final master command file
export ssh_key_file=$remote_private_key_file
export own_public_ip=$master_ip
export master_port
export status_file=$remote_status_file
export ready_file=$remote_ready_file
envsubst '$ssh_key_file $own_public_ip $master_port $status_file $ready_file' < "$exp_data_dir/$local_master_command_template_file" > "$exp_data_dir/$local_master_command_file"
echo -e "\nwrite-file $status_file DONE" >> "$exp_data_dir/$local_master_command_file"

# Create remote code, config, and experiment directories
ssh $ssh_options root@$master_ip "
  mkdir -p $remote_code_dir &&
  mkdir -p $remote_config_dir &&
  mkdir -p $remote_exp_dir/raw-results" || exit 1

# Upload code to the master
rsync --progress -rptz -e "ssh $ssh_options" $local_code_files "root@$master_ip:$remote_code_dir" || exit 2

# Upload config to the master
rsync --progress -rptz -e "ssh $ssh_options" $exp_data_dir/config/* "root@$master_ip:$remote_config_dir" || exit 3

# Upload analysis scripts and queries to the master
rsync --progress -rptz -e "ssh $ssh_options" queries scripts "root@$master_ip:$remote_work_dir" || exit 4

# Upload commands to the master
scp $ssh_options "$exp_data_dir/$local_master_command_file" "root@$master_ip:$remote_master_command_file" || exit 5

# Generate TLS certificates and compile code at the master
ssh $ssh_options root@$master_ip "
  cd $remote_tls_directory &&
  ./generate.sh $master_ip &&
  cd $remote_work_dir &&
  cp -r $remote_tls_directory . &&

  echo 'Compiling mir-modular.' &&
  export PATH=\$PATH:$remote_gopath/bin:$remote_work_dir/bin &&
  export GOPATH=$remote_work_dir/go
  # Disabling go modles to be able to compile with new Go version (>=1.16.3)
  export GO111MODULE=auto
  export GOCACHE=/root/.cache/go-build
  cd $remote_code_dir &&
  ./run-protoc.sh &&
  go install ./cmd/... &&


## Run threshold signature microbenchmark
#ssh $ssh_options root@$master_ip "
#  echo 'Running threshold signature microbenchmark.' &&
#  cd $remote_code_dir/crypto/ &&
#  go test -bench=TBLS" || exit 7

# Start master server
echo "Starting result processor and master server."
ssh $ssh_options root@$master_ip "
  ulimit -Sn $open_files_limit &&
  $remote_work_dir/scripts/analyze/analyze-continuously.sh $remote_exp_dir $remote_status_file $remote_work_dir/scripts $remote_work_dir/queries $remote_gopath/bin/orderingpeer $remote_gopath/bin/orderingclient $remote_analysis_processes > $remote_exp_dir/continuous-analysis.log 2>&1 &
  export PATH=\$PATH:$remote_gopath/bin:$remote_work_dir/bin &&
  discoverymaster $master_port file $remote_master_command_file > $remote_master_log 2>&1 < /dev/null"
