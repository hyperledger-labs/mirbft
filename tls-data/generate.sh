#!/bin/bash
#
# usage: generate.sh [-f] [IPaddr [IPaddr [...]]]
# Generate CA certificate (-f to force overwrite if already exists)
# and a certificate to authenticate the local peer (with the local IP address(es))
# Also generate keys for client authentication if -f option is given.

# Generate CA certificate.
if [ "$1" = "-f" ]; then
  shift
  ./generate-ca.sh -f
  ./generate-client.sh "$@"
else
  ./generate-ca.sh
fi

# Generate certificate to authenticate this node.
./generate-auth.sh "$@"
