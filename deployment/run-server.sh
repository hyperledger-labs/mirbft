#!/bin/bash -e

echo "Killing previously running servers and clients"
pkill server
pkill client

echo "Chancing working directory"
cd /opt/gopath/src/github.com/IBM/mirbft/server


echo "Starting server"
./server ../deployment/config/serverconfig/config.yml server &> server.out &