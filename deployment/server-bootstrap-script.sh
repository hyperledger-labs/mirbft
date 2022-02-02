#!/bin/bash
work_dir=/root
log_file=$work_dir/main_log.log
status_file=$work_dir/status
user_script_uploaded=$work_dir/user-script-uploaded
user_script_body=$work_dir/user-script-body.sh
{
# Optimization. Not strictly needed.
echo "PRE-INSTALLING" > $status_file
apt-get -y update
apt-get -y install git openssl jq graphviz

echo "WAITING" > $status_file
echo "Waiting for instance detail to be uploaded."
while ! [[ -r "$user_script_uploaded" ]]; do
    sleep 1
done
source $user_script_body
} >> $log_file 2>> $log_file