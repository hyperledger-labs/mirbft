#!/bin/bash

source scripts/global-vars.sh

ssh $ssh_options -q  -o "ConnectTimeout=10" "root@$1" "cat $remote_status_file"
