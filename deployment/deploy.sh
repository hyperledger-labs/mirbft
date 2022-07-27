#!/bin/bash -e

getIP() {
	grep -w $1 cloud-instance.info | awk '{ print $2}'
}

servers=$(grep server cloud-instance.info | awk '{ print $1}')
clients=$(grep client cloud-instance.info | awk '{ print $1}')

. vars.sh

if [ "$1" = "--pull-only" ] || [ "$1" = "-p" ]; then
  pull=true
  shift
else
  pull=false
fi

for p in $servers $clients; do
         pub=$(getIP $p)
         if [ "$pull" = "false" ]; then
            scp $ssh_options clone.sh $ssh_user@$pub:
            scp $ssh_options install-local.sh $ssh_user@$pub:
            scp $ssh_options vars.sh $ssh_user@$pub:
            ssh $ssh_user@$pub $ssh_options "source install-local.sh"
         fi
         ssh $ssh_user@$pub $ssh_options "source clone.sh"
done