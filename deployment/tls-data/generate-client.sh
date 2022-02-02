#!/bin/bash

# Key and certificate of the certificate authority (CA)
cakey="ca.key"
cacert="ca.pem"

# Prefix of the generated files
fileprefix="client"

# Basic configuration file (will be copied to the used config file)
conffile="openssl.conf"
# Name for a the generated config file (basic config file, potentially with more addresses appended)
localconffile="openssl-local.conf"

function generate_cert() {
  local keyfile=$1
  local requestfile=$2
  local certfile=$3

  echo
  echo "Generating certificate signing request (CSR): $requestfile"
  openssl req -new -key "$keyfile" -out "$requestfile" -config "$localconffile"

  echo
  echo "Generating certificate: $certfile"
  openssl x509 -req -in "$requestfile" -CA "$cacert" -CAkey "$cakey" -out "$certfile" -days 365 -sha256 -extfile "$localconffile" -extensions req_ext -CAcreateserial # -addtrust clientAuth
  # According to https://gist.github.com/ncw/9253562 : Adding -addtrust clientAuth makes certificates Go can't read
}

function generate_rsa() {

  local keysize=$1
  local keyfile="$fileprefix-rsa-$keysize.key"
  local requestfile="$fileprefix-rsa-$keysize.csr"
  local certfile="$fileprefix-rsa-$keysize.pem"

  echo
  echo "Generating key and certificate: $keyfile, $certfile"

  # Generate the key and convert it to PKCS8 format, so the Go program can read it.
  echo
  echo "Generating key: $keyfile"
  openssl genrsa -out "$keyfile.tmp" $keysize
  openssl pkcs8 -topk8 -inform pem -in "$keyfile.tmp" -outform pem -nocrypt -out $keyfile
  rm "$keyfile.tmp"

  generate_cert $keyfile $requestfile $certfile
}

function generate_ecdsa() {

  local keysize=$1
  local keyfile="$fileprefix-ecdsa-$keysize.key"
  local requestfile="$fileprefix-ecdsa-$keysize.csr"
  local certfile="$fileprefix-ecdsa-$keysize.pem"
  local curvename

  if [ $keysize -eq 224 ]; then
    curvename="secp224r1"
  elif [ $keysize -eq 256 ]; then
    curvename="prime256v1"
  elif [ $keysize -eq 384 ]; then
    curvename="secp384r1"
  else
    >&2 echo "generate-client.sh: unsupported ECDSA key size: $keysize (supported values: 224, 256, 384)"
    return
  fi

  echo
  echo "Generating key and certificate: $keyfile, $certfile"

  # Generate the key and convert it to PKCS8 format, so the Go program can read it.
  echo
  echo "Generating key: $keyfile"
  openssl ecparam -out "$keyfile.tmp" -name $curvename -genkey
  openssl pkcs8 -topk8 -inform pem -in "$keyfile.tmp" -outform pem -nocrypt -out $keyfile
  rm "$keyfile.tmp"

  generate_cert $keyfile $requestfile $certfile
}


# Generate openssl config file as a copy of the base config file, plus additional addresses.
cat "$conffile" > "$localconffile"
ipcount=2
while [ -n "$1" ]; do
  ipcount=$((ipcount + 1))
  echo "Adding IP address to certificate alt names IP.$ipcount: $1"
  echo "IP.$ipcount = $1" >> "$localconffile"
  shift
done

generate_rsa 2048
generate_rsa 4096
generate_ecdsa 224
generate_ecdsa 256
generate_ecdsa 384
