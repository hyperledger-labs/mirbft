#!/bin/bash

role=$1
discovery_addr=$2
template_file=$3
config_file=$4
shift 4

ssh_options="-i ibmcloud-ssh-key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

if [ "$role" = "peer" ]; then

  output_file=$1
  own_public_ip=$2
  own_private_ip=$3
  shift 3

  response=$(oldmirdiscovery "$role" "$discovery_addr" "$own_public_ip" "$own_private_ip")

  own_id=$(echo "$response" | jq -r '.OwnID')
  listen_port=$(echo "$response" | jq -r '.ListenPort')
  num_nodes=$(echo "$response" | jq -r '.NumPeers')
  num_faults=$(echo "$response" | jq -r '.NumFaults')
  addresses=$(echo "$response" | jq -r '.Addresses[]')

  cat "$template_file" | sed "s/SERVER_ID/$own_id/ ; s/LISTEN_ENDPOINT/0.0.0.0:$listen_port/ ; s/NUM_NODES/$num_nodes/ ; s/NUM_FAULTS/$num_faults/" > "$config_file"
  echo "  addresses:" >> $config_file
  for endpoint in $addresses; do
    echo "  - \"$endpoint\"" >> $config_file
  done
  echo "  addresses:" >> $config_file
  for endpoint in $addresses; do
    address="$(cut -d':' -f1 <<<$endpoint)"
    scp $ssh_options root@$address:/root/tls-data/auth.pem /root/tls-data/$address.pem
    echo "  - \"/root/tls-data/$address.pem\"" >> $config_file
  done

  server "$config_file" > "$output_file" 2>&1

elif [ "$role" = "client" ]; then

  response=$(oldmirdiscovery "$role" "$discovery_addr")

  num_nodes=$(echo "$response" | jq -r '.NumPeers')
  num_faults=$(echo "$response" | jq -r '.NumFaults')
  addresses=$(echo "$response" | jq -r '.Addresses[]')

  cat "$template_file" | sed "s/NUM_NODES/$num_nodes/ ; s/NUM_FAULTS/$num_faults/" > "$config_file"
  for endpoint in $addresses; do
    echo "  - \"$endpoint\"" >> $config_file
  done

else
  >&2 echo "Invalid role: $role. Need 'client' or 'peer'"
fi
