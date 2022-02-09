#!/bin/bash

. vars.sh

echo "Installing Ubuntu packages."

sudo add-apt-repository -y ppa:longsleep/golang-backports

sudo apt-get -y update
sudo apt-get -y install \
	protobuf-compiler \
	protobuf-compiler-grpc \
	git \
	openssl \
	jq \
	graphviz

cd ~

echo "Installing golang."

wget https://storage.googleapis.com/golang/go1.17.2.linux-amd64.tar.gz
tar xpzf go1.17.2.linux-amd64.tar.gz

sudo mkdir -p /opt/gopath
sudo chown -R  $user:$group /opt/gopath

export PATH=$PATH:~/go/bin/:/opt/gopath/bin/
export GOPATH=/opt/gopath
export GOROOT=~/go
export GOCACHE=~/.cache/go-build
export GIT_SSL_NO_VERIFY=1
export GO111MODULE=off

cat << EOF >> ~/.bashrc
export PATH=$PATH:~/go/bin/:/opt/gopath/bin/
export GOPATH=/opt/gopath
export GOROOT=~/go
export GOCACHE=~/.cache/go-build
export GIT_SSL_NO_VERIFY=1
export GO111MODULE=off
EOF

echo "Installing golang packages. (May take a long time without producing output.)"

echo "Installing gRPC for Go."
go get -u google.golang.org/grpc

echo "Installing Protobufs for Go."
go get -u github.com/golang/protobuf/protoc-gen-go

echo "Installing Zerolog for Go."
go get -u github.com/rs/zerolog/log

echo "Installing Linux Goprocinfo for Go"
go get -u github.com/c9s/goprocinfo/linux

echo "Installing Kyber for Go"
go get -u go.dedis.ch/kyber
go get go.dedis.ch/fixbuf
go get golang.org/x/crypto/blake2b

echo "Installing the YAML parser for Go"
go get -u gopkg.in/yaml.v2