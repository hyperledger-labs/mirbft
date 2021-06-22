#!/bin/bash -e

getIP() {
	grep -w $1 cloud-instance.info | awk '{ print $2}'
}

servers=$(grep server cloud-instance.info | awk '{ print $1}')
clients=$(grep client cloud-instance.info | awk '{ print $1}')
C=$(grep -c client cloud-instance.info)

. vars.sh

for p in $clients; do
    pub=$(getIP $p)
    scp $ssh_options run-client.sh $user@$pub:
    ssh $user@$pub $ssh_options "source run-client.sh"
done

for p in $servers; do
    pub=$(getIP $p)
    scp $ssh_options run-server.sh $user@$pub:
    scp $ssh_options stop.sh $user@$pub:
done

for p in $servers; do
    pub=$(getIP $p)
    ssh $user@$pub $ssh_options "source run-server.sh" &
done

ready="0"
while [ $ready -lt $C ]; do
    for p in $clients; do
        pub=$(getIP $p)
        scp $ssh_options $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/client/STATUS.sh .
        . STATUS.sh
        echo $p $status
        if [ "$status" = "FINISHED" ]; then
            ready=$[$ready+1]
        fi
    done
    if [ $ready -lt $C ]; then
        ready="0"
    fi
    echo "Experiment still running"
    sleep 5
done

rm STATUS.sh

echo "All clients finished"

mkdir -p experiment-output

for p in $servers; do
    pub=$(getIP $p)
    ssh $user@$pub $ssh_options "source stop.sh" &
    scp $ssh_options $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/server/server.out experiment-output/$p.out
done

echo "All servers stopped, server log files are copied in deployment/experiment-output/"

for p in $clients; do
    pub=$(getIP $p)
    scp $ssh_options $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/client/client.out experiment-output/$p.out
    scp $ssh_options $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/client/*trc experiment-output
done

echo "All servers stopped, client trace and log files are copied in deployment/experiment-output/"
