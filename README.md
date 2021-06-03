# MirBFT Research Prototype
The implementation for  [Mir-BFT: High-Throughput Robust BFT for Decentralized Networks
](https://arxiv.org/abs/1906.05552) paper.

## Setup
The following scripts among other dependencies install `Go` in the home directory and set gopath to `/opt/gopath/bin/`.

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

To compile the node:

`cd server`

`go build`

To compile the client:

`cd client`

`go build`

A node sample configuration exists in `sampleconfig/serverconfig/` .

A client sample configuration exists in `sampleconfig/clientconfig/`.

To start locally a setup with 4 nodes and 1 client:

On each node:

`cd server`

`./server ../sampleconfig/serverconfig/config$id.yml server$id`
where `$id` is `1 2 3 4` for each of the 4 nodes.

The first argument is the path to the node configuration and the second argument a name prefix for a trace file.


On the client:

`cd client`

`./client ../sampleconfig/clientconfig/4peer-config.yml client`

Again, the first argument is the path to the node configuration and the second argument a name prefix for a trace file.


Similarly, to start locally a setup with 1 peer and 1 client:

On the node:

`cd server`

`./server ../sampleconfig/serverconfig/config.yml server`

On the client:

`cd client`

`./client ../sampleconfig/clientconfig/1peer-config.yml client`


