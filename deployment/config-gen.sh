#!/bin/bash -e

getIP() {
    if [ "$local" = "true" ]; then
        echo 127.0.0.1
    else
	    grep -w $1 cloud-instance.info | awk '{ print $2}'
	fi
}

getPrivIP(){
    if [ "$local" = "true" ]; then
        echo 127.0.0.1
    else
	    grep -w $1 cloud-instance.info | awk '{ print $3}'
	fi
}

genCert(){
    openssl ecparam -name secp521r1 -genkey -param_enc named_curve -out $1.key
    openssl ecparam -genkey -name prime256v1 -out $1.key
    openssl req -new -SHA384 -key $1.key -nodes -out $1.csr -config openssl.conf
    openssl x509 -req -SHA384 -days 3650 -in $1.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out $1.pem -extfile openssl.conf -extensions req_ext -CAcreateserial
}

. vars.sh

if [ "$1" = "--local" ] || [ "$1" = "-l" ]; then
  local=true
  shift

  N=$1
  F=$(((N-1)/3))
  shift

  C=$1
  shift

  servers=""
  clients=""
  for i in $(seq 1 $N); do
    servers+="server$i "
  done
  for i in $(seq 1 $C); do
      clients+="client$i "
  done
else
  local=false
  shift

  servers=$(grep server cloud-instance.info | awk '{ print $1}')
  clients=$(grep client cloud-instance.info | awk '{ print $1}')
  N=$(grep -c server cloud-instance.info)
  F=$(((N-1)/3))
  C=$(grep -c client cloud-instance.info)
fi

if [ "$1" = "--config-only" ] || [ "$1" = "-c" ]; then
  config_only=true
  shift
else
  config_only=false
fi


echo "Removing old configuration"
rm -rf temp
mkdir -p temp

if [ "$local" = "true" ]; then
  if [ "$config_only" = "false" ]; then
    rm -rf /opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/*
  fi
  rm -rf /opt/gopath/src/github.com/IBM/mirbft/deployment/config/serverconfig/*
  rm -rf /opt/gopath/src/github.com/IBM/mirbft/deployment/config/clientconfig/*
  mkdir -p /opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/
  mkdir -p /opt/gopath/src/github.com/IBM/mirbft/deployment/config/serverconfig/
  mkdir -p /opt/gopath/src/github.com/IBM/mirbft/deployment/config/clientconfig/
else
  for p in $servers $clients; do
      pub=$(getIP $p)
      if [ "$config_only" = "false" ]; then
        ssh $user@$pub $ssh_options "rm -rf /opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/*"
      fi
      ssh $user@$pub $ssh_options "rm -rf /opt/gopath/src/github.com/IBM/mirbft/deployment/config/serverconfig/*"
      ssh $user@$pub $ssh_options "rm -rf /opt/gopath/src/github.com/IBM/mirbft/deployment/config/clientconfig/*"
      ssh $user@$pub $ssh_options "mkdir -p /opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/"
      ssh $user@$pub $ssh_options "mkdir -p /opt/gopath/src/github.com/IBM/mirbft/deployment/config/serverconfig/"
      ssh $user@$pub $ssh_options "mkdir -p /opt/gopath/src/github.com/IBM/mirbft/deployment/config/clientconfig/"
  done
fi

if [ "$config_only" = "false" ]; then
    cd temp
    ../generate-ca.sh -f

    echo "Generating Certificates"
    for p in $servers; do
        pub=$(getIP $p)
        priv=$(getIP $p)
        cat ../config-file-templates/openssl-template.conf | sed "s/PUB-IP/$pub/ ; s/PRIV-IP/$priv/"> openssl.conf
        genCert $p
    done

    rm openssl.conf


    echo "Copying Certificates"
    if [ "$local" = "true" ]; then
        cp * /opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/
    else
        for p in $servers $clients; do
            pub=$(getIP $p)
            scp $ssh_options *.pem $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/
        done
        for p in $servers; do
            pub=$(getIP $p)
            scp $ssh_options $p.key $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/
        done
    fi
    cd ..
fi

echo "Generating configuration files"
port=8001
id=0
thres=16
if [ $N -ge $thres ]; then
    watermark=$(( N*2 ))
    epoch=$(( N*16 ))
    timeout=$(( N*32*1000000 ))
else
    watermark=32
    epoch=256
    timeout=500000000
fi

for p in $servers; do
    ip=$(getPrivIP $p)
    cat config-file-templates/server-config.yml | sed "s/SERVER_ID/$id/ ; s/LISTEN_ENDPOINT/0.0.0.0:$port/ ; s/SERVER_HOSTNAME/$p/ ; s/NUM_NODES/$N/ ; s/NUM_FAULTS/$F/ ; s/EPOCH/$epoch/ ; s/WATERMARK/$watermark/ ; s/BATCHTIMEOUT/$timeout/"> temp/config_$p.yml
    (( id += 1 ))
    if [ "$local" = "true" ]; then
        (( port += 10 ))
    fi
done

id=0
for p in $clients; do
    cat config-file-templates/client-config.yml | sed "s/CLIENT_ID/$id/ ; s/NUM_NODES/$N/ ; s/NUM_FAULTS/$F/ ; s/NUM_CLIENTS/$C/ ; s/EPOCH/$epoch/"> temp/config_$p.yml
    (( id += 1 ))
done

for p in $servers; do
    echo "  certFiles:" >> temp/config_$p.yml
    for s in $servers; do
        echo "    - \"/opt/gopath/src/github.com/IBM/mirbft/deployment/config/certs/ecdsa/$s.pem\"" >> temp/config_$p.yml
    done
    echo "  addresses:" >> temp/config_$p.yml
    port=8001
    for s in $servers; do
        ip=$(getPrivIP $s)
        if [ "$local" = "true" ]; then
            ip="127.0.0.1"
        fi
        echo "    - \"$ip:$port\"" >> temp/config_$p.yml
        if [ "$local" = "true" ]; then
            (( port += 10 ))
        fi
    done
done

for p in $clients; do
    echo "  addresses:" >> temp/config_$p.yml
    port=8001
    (( port += 2 ))
    for s in $servers; do
        ip=$(getIP $s)
        if [ "$local" = "true" ]; then
            ip="127.0.0.1"
        fi
        echo "    - \"$ip:$port\"" >> temp/config_$p.yml
        if [ "$local" = "true" ]; then
            (( port += 10 ))
        fi
    done
done

echo "Copying configuration files"
if [ "$local" = "true" ]; then
    for p in $servers; do
       cp temp/config_$p.yml /opt/gopath/src/github.com/IBM/mirbft/deployment/config/serverconfig/
    done
    for p in $clients; do
       cp temp/config_$p.yml /opt/gopath/src/github.com/IBM/mirbft/deployment/config/clientconfig/
    done
else
    for p in $servers; do
       pub=$(getIP $p)
       scp $ssh_options temp/config_$p.yml $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/deployment/config/serverconfig/config.yml
    done
    for p in $clients; do
       pub=$(getIP $p)
       scp $ssh_options temp/config_$p.yml $user@$pub:/opt/gopath/src/github.com/IBM/mirbft/deployment/config/clientconfig/config.yml
    done
fi

rm -rf temp