#!/bin/bash

. vars.sh

sudo apt-get update
sudo apt-get install -y curl git
sudo apt-get install -y python-minimal
sudo apt-get install -y python-numpy
sudo apt-get install -y build-essential
sudo apt-get install -y protobuf-compiler
sudo apt-get install -y	protobuf-compiler-grpc

cd ~

wget https://storage.googleapis.com/golang/go1.14.2.linux-amd64.tar.gz
tar xpzf go1.14.2.linux-amd64.tar.gz

sudo mkdir -p /opt/gopath
sudo chown -R  $user:$group /opt/gopath

export PATH=$PATH:~/go/bin/:/opt/gopath/bin/
export GOPATH=/opt/gopath
export GOROOT=~/go

cat << EOF >> ~/.bashrc
export PATH=$PATH:~/go/bin/:/opt/gopath/bin/
export GOPATH=/opt/gopath
export GOROOT=~/go
EOF

go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
go get -u github.com/op/go-logging
go get -u golang.org/x/net/context
go get -u  gopkg.in/yaml.v2
go get -u github.com/rs/zerolog/log
