initial_directory=$(pwd)

# Kill all application processes on the local machine
echo "Killing all application processes on the local machine."
killall discoverymaster discoveryslave orderingpeer orderingclient

echo "Compiling."
cd ../ || exit 1
./run-protoc.sh || exit 1
cd $initial_directory || exit 1
go install -race ../cmd/... || exit 1

echo "Copy TLS keys and certificates to $exp_data_dir"
cp -r tls-data $exp_data_dir || exit 1

echo "Changing directory to $exp_data_dir"
cd $exp_data_dir || exit 1

# Master command file does not need any modification when generated for local use.
cp "$local_master_command_template_file" "$local_master_command_file"

# Start master server
echo "Starting master server."
discoverymaster $master_port file $local_master_command_file > $local_master_log 2>&1 < /dev/null &
echo "RUNNING" > $local_master_status_file

echo "Changing directory back to $initial_directory"
cd $initial_directory || exit 1

echo "Copying code and binaries to experiment data directory for later analysis."
mkdir -p "$exp_data_dir/gopath/src/$downloaded_code_dir"
mkdir -p "$exp_data_dir/gopath/bin"
cp -r $local_code_files $exp_data_dir/gopath/src/$downloaded_code_dir/
cp $GOPATH/bin/orderingpeer $GOPATH/bin/orderingclient $exp_data_dir/gopath/bin/

# Start slaves according to schedule
scripts/deploy-slaves-local.sh "$exp_data_dir" $deploy_schedule &

echo "Waiting for local experiment to finish."
wait

echo "Analyzing experiments."
scripts/analyze/analyze-parallel.sh $analysis_query_params $exp_data_dir/experiment-output/*