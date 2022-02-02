deployment_data_root=deployment-data

dpl_filename=deployment.dpl
csv_filename=deployment.csv
result_summary_file=result-summary.csv
exp_id_digits=4
analysis_query_params="-q queries/ethereum.sql -q queries/aggregates.sql -q queries/histograms.sql"

# Private key, of which the corresponding public key needs to be an authorized ssh key at each instance.
# (Previously uploaded to IBM Cloud and specified at instance creation in the corresponding template file)
private_key_file=ibmcloud-ssh-key

# Options to use when communicating with the remote machines.
ssh_options="-i $private_key_file -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"

# Command to kill children of exiting scripts
trap_exit_command='{ jobs; if [ -n "$(jobs -p)" ]; then kill $(jobs -p); fi; sleep 0.5; } > /dev/null 2>&1'

# Port on which the master listens.
master_port=9999
machine_status_poll_period=5

# master system requirements
#master_machine=cloud-machine-templates/dedicated-machine-32-CPUs-32GB-RAM-lon02.cmt
#master_machine=cloud-machine-templates/dedicated-machine-32-CPUs-32GB-RAM-ams01.cmt
master_machine=cloud-machine-templates/dedicated-machine-32-CPUs-32GB-RAM-fra02.cmt
#master_machine=cloud-machine-templates/dedicated-machine-32-CPUs-32GB-RAM-mil01.cmt
#master_machine=cloud-machine-templates/small-machine-mil01.cmt
#master_machine=cloud-machine-templates/small-machine-fra02.cmt


# The maximum number of open files to be set at remote machines.
open_files_limit=16384

# Number of seconds to wait for a deployed instance to become ready.
# This could be provided as the --wait option to the instance creation command,
# but it often makes the CLI command fail (buggy implementation of either the CLI or the IBM Cloud backend).
# Instead, the instance ready state is queried separately.
instance_ready_timeout=600
instance_creation_batch=64
instance_info_file_name=cloud-instance-info
default_instance_info=last-cloud-instance-info

local_public_ip=127.0.0.1
local_private_ip=127.0.0.1
local_master_command_template_file=master-commands-template.cmd
local_master_command_file=master-commands.cmd
local_master_log=master-log.log
local_master_status_file=master-status
local_master_ready_file=master-ready
local_result_fetching_log=result-fetching.log
remote_work_dir=/root
remote_instance_tag_file=$remote_work_dir/instance-tag
remote_status_file=$remote_work_dir/status
remote_ready_file=$remote_work_dir/master-ready
remote_main_log=/root/main_log.log
remote_master_log=/root/master-log.log
remote_slave_log=/root/slave-log.log

# Key used by the instances to communicate among each other.
remote_private_key_file=$remote_work_dir/ibmcloud-ssh-key

remote_instance_detail_file=$remote_work_dir/instance-detail.json
remote_user_script_body=$remote_work_dir/user-script-body.sh
remote_user_script_uploaded=$remote_work_dir/user-script-uploaded
remote_master_command_file=$remote_work_dir/master-commands.cmd
remote_exp_dir=$remote_work_dir/current-deployment-data
remote_analysis_processes=8

remote_gopath=/root/go
remote_code_dir="$remote_gopath/src/github.com/hyperledger-labs/mirbft"
remote_config_dir=$remote_work_dir/experiment-config
remote_tls_directory="$remote_code_dir/tls-data"
remote_log_archives="experiment-output-*.tar.gz"
downloaded_code_dir=github.com/hyperledger-labs/mirbft/
# remote_delete_files must be on one line, as it is passed in a string to SSH as a command to execute
remote_delete_files="$remote_work_dir/experiment-output-*.tar.gz $remote_work_dir/experiment-output $remote_master_log $remote_slave_log $remote_status_file $remote_ready_file $remote_instance_tag_file $remote_master_command_file $remote_code_dir $remote_config_dir $remote_exp_dir"
downloaded_gopath="remote-gopath"

server_bootstrap_script=server-bootstrap-script.sh
user_script_template_slave=user-script-slave.sh.template
user_script_template_master=user-script-master.sh.template

local_code_dir=".."
local_code_files="
$local_code_dir/announcer
$local_code_dir/checkpoint
$local_code_dir/cmd
$local_code_dir/config
$local_code_dir/crypto
$local_code_dir/discovery
$local_code_dir/log
$local_code_dir/manager
$local_code_dir/membership
$local_code_dir/messenger
$local_code_dir/orderer
$local_code_dir/profiling
$local_code_dir/request
$local_code_dir/protobufs
$local_code_dir/statetransfer
$local_code_dir/tls-data
$local_code_dir/tracing
$local_code_dir/util
$local_code_dir/validator
$local_code_dir/run-protoc.sh"
