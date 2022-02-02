#!/bin/bash

# Private key, of which the corresponding public key needs to be an authorized ssh key at each instance.
# (Previously uploaded to IBM Cloud and specified at instance creation in the corresponding template file)
private_key_file=ibmcloud-ssh-key

# Options to use when communicating with the remote machines.
ssh_options="-i $private_key_file -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60"

experiments=1
clientProcesses=1
clientThreads=2
duration=5
payloadSize=1
nDummyMaps=0
ownIP=""
remoteClientsFile=""
clientTags=""
remoteClients=""
contentionTestBinary="go/bin/contentiontest"

while [ -n "$1" ]; do
  if [ "$1" = "-n" ]; then
    experiments=$2
    shift
  elif [ "$1" = "-d" ]; then
    duration=$2
    shift
  elif [ "$1" = "-p" ]; then
    clientProcesses=$2
    shift
  elif [ "$1" = "-t" ]; then
    clientThreads=$2
    shift
  elif [ "$1" = "--remote-clients" ]; then
    remoteClientsFile=$2
    shift
  elif [ "$1" = "--own-ip" ]; then
    ownIP=$2
    shift
  elif [ "$1" = "--client-tag" ]; then
    clientTags="$clientTags $2"
    shift
  elif [ "$1" = "-b" ]; then
    contentionTestBinary=$2
    shift
  elif [ "$1" = "-l" ]; then
    payloadSize=$2
    shift
  elif [ "$1" = "-m" ]; then
    nDummyMaps=$2
    shift
  else
    >&2 echo "invalid argument: $1"
    exit 1
  fi
  shift
done

function pushBinary() {
  local clIp=$1

  echo "Uploading binary to client machine and killing test: $clIp"

  ssh $ssh_options root@$clIp "killall contentiontest"

  # Upload binary to client
  scp $ssh_options $contentionTestBinary root@$clIp:
}


function runRemoteClient() {
  local clIp=$1

  ssh $ssh_options -o "LogLevel=QUIET" root@$clIp "./contentiontest client $ownIP --tls tls-data -n $clientThreads -t $duration -l $payloadSize $channelOptions > contention-test-client.log"
}

for tag in $clientTags; do
  newClients=$(awk "\$4 == \"$tag\" {print \$2}" $remoteClientsFile)
  remoteClients="$remoteClients $newClients"
done

if [ -n "$remoteClients" ] && [ -z "$ownIP" ]; then
  >&2 echo "Must specify own IP when using remote clients."
  exit 2
fi

echo "experiments: $experiments"
echo "clientProcesses: $clientProcesses"
echo "clientThreads: $clientThreads"
echo "duration: $duration"
echo "remote clients:"
for cl in $remoteClients; do
  echo "    $cl"
done
echo ""

echo "         #, total-rate,    clients,  bandwidth,  clients-0,     rate-0,  clients-1,     rate-1,   contention" > contention-test.csv

for c in $remoteClients; do
  pushBinary $c &
done
wait

ulimit -Sn 16384

i=0
for channelOptions in "-c 0 -c 1" "-c 0"; do
  echo -e "\nTesting channels: $channelOptions\n"
  for e in $(seq 1 $experiments); do
    echo -n "Running experiment: $e / $experiments"
    printf "% 10d, " $i >> contention-test.csv
    $contentionTestBinary server --tls tls-data -m $nDummyMaps -f contention-test.csv >> contention-test.log &
    if [ -n "$remoteClients" ]; then
      for c in $remoteClients; do
        runRemoteClient $c &
      done
    else
      for c in $(seq 1 $clientProcesses); do
        $contentionTestBinary client 127.0.0.1 --tls tls-data -n $clientThreads -t $duration -l $payloadSize $channelOptions > /dev/null &
      done
    fi
    wait
    echo " DONE."
    go tool pprof --pdf ~/go/bin/contentiontest contention-test.mutex > contention-test-$i.pdf
    go tool pprof --list=.* ~/go/bin/contentiontest contention-test.mutex | grep -iE '\s+[0-9]+(\.[0-9]+)?.+\s116:\s*b.mu.Unlock()' | awk '{print "   " $2}' >> contention-test.csv
    i=$((i+1))
  done
done
