#!/bin/bash

# Key and certificate of the certificate authority (CA)
cakey="ca.key"
cacert="ca.pem"

# Names of the generated files
keyfile="auth.key"
requestfile="auth.csr"
certfile="auth.pem"

# Basic configuration file (will be copied to the used config file)
conffile="openssl.conf"
# Name for a the generated config file (basic config file, potentially with more addresses appended)
localconffile="openssl-local.conf"

echo
echo "Generating key and certificate: $keyfile, $certfile"

# Generate openssl config file as a copy of the base config file, plus additional addresses.
cat "$conffile" > "$localconffile"
ipcount=2
while [ -n "$1" ]; do
  ipcount=$((ipcount + 1))
  echo "Adding IP address to certificate alt names IP.$ipcount: $1"
  echo "IP.$ipcount = $1" >> "$localconffile"
  shift
done

# Generate key and certificate.

echo
echo "Generating key: $keyfile"
openssl genrsa -out "$keyfile.tmp" 4096
openssl pkcs8 -topk8 -inform pem -in "$keyfile.tmp" -outform pem -nocrypt -out $keyfile
rm "$keyfile.tmp"

echo
echo "Generating certificate signing request (CSR): $requestfile"
openssl req -new -key "$keyfile" -out "$requestfile" -config "$localconffile"

echo
echo "Generating certificate: $certfile"
openssl x509 -req -in "$requestfile" -CA "$cacert" -CAkey "$cakey" -out "$certfile" -days 365 -sha256 -extfile "$localconffile" -extensions req_ext -CAcreateserial # -addtrust clientAuth
# According to https://gist.github.com/ncw/9253562 : Adding -addtrust clientAuth makes certificates Go can't read
