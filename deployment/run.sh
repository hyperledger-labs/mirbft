#!/bin/bash -e

getIP() {
	grep -w $1 cloud-instance.info | awk '{ print $2}'
}

servers=$(grep server cloud-instance.info | awk '{ print $1}')
clients=$(grep client cloud-instance.info | awk '{ print $1}')
C=$(grep -c client cloud-instance.info)

. vars.sh

if [ "$1" = "--copy-only" ] || [ "$1" = "-c" ]; then
  copy_only=true
  shift
else
  copy_only=false
fi

if [ "$copy_only" = "false" ]; then
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
        ssh $user@$pub $ssh_options "source run-server.sh  > /dev/null 2>&1 & " &
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
fi

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
    if ssh $user@$pub $ssh_options stat /opt/gopath/src/github.com/IBM/mirbft/client/*trc \> /dev/null 2\>\&1; then
        scp -r $ssh_options $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/client/*trc experiment-output
    else
        echo "Client trace file does not exist. Client $p did not finish gracefully."
    fi
done

echo "All clients stopped, client trace and log files are copied in deployment/experiment-output/"
