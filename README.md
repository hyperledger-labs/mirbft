# MirBFT Research Prototype
The implementation for  [Mir-BFT: High-Throughput Robust BFT for Decentralized Networks
](https://arxiv.org/abs/1906.05552) paper.

## Setup
The following scripts among other dpendencies install `Go` in the home directory and set gopath to `/opt/gopath/bin/`.

The default path to the repository is set to: `/opt/gopath/src/github.com/IBM/mirbft/`.


Go to the deployment directory:

`cd deployment`

To install Golang and requirements: 

`./install-local.sh`

To clone the repository under `/opt/gopath/src/github.com/IBM/mirbft/`:

`./clone.sh`

Build the protobufs:

`cd ..`

`./run-protoc.sh`

To compile the peer:

`cd server`

`go build`

To compile the client:

`cd client`

`go build`

A peer sample configuration exists in `sampleconfig/serverconfig/` .

A client sample configuration exists in `sampleconfig/clientconfig/`.

To start locally a setup with 4 peers and 1 client on each peer:

`cd server`

`./server config$id`
where `$id` is `1 2 3 4` for each of the 4 peers

On the client:

`cd client`

`./client 4peer-config`

To start locally a setup with 1 peer and 1 client:

`cd server`

`./server config`

On the client:

`cd client`

`./client 1peer-config`

