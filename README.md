# MirBFT Library

This open-source project is part of [Hyperledger Labs](https://labs.hyperledger.org/labs/mir-bft.html).

It aims at developing a production-quality implementation of:
- a general framework for easily implementing distributed protocols
- the [ISS](/pkg/iss) Byzantine fault tolerant consensus protocol.

MirBFT is intended to be used as a scalable and efficient [consensus layer in Filecoin subnets](https://github.com/protocol/ConsensusLab/issues/9)
and, potentially, as a Byzantine fault tolerant ordering service in Hyperledger Fabric.

## Overview

MirBFT is a library that provides a general framework for implementing distributed algorithms
in a network transport, storage, and cryptographic algorithm agnostic way.
MirBFT hopes to be a building block of a next generation of distributed systems, being used by many applications.

### Insanely Scalable State Machine Replication

The first algorithm to be implemented in MirBFT is called [ISS (Insanely Scalable SMR)](/pkg/iss),
a state-of-the-art [atomic broadcast](https://en.wikipedia.org/wiki/Atomic_broadcast) protocol
which can be utilized by any distributed system.

ISS improves on traditional atomic broadcast protocols
like [PBFT](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/p398-castro-bft-tocs.pdf)
and [Raft](https://raft.github.io/raft.pdf),
which always have a single active leader that proposes batches of requests for ordering,
by allowing multiple leaders to do so concurrently, without giving up total order guarantees.
The multi-leader nature of ISS leads to exceptional performance, especially on wide area networks,
but should be suitable for LAN deployments as well.

### Structure and Usage

MirBFT is a framework for implementing distributed algorithms (also referred to as distributed protocols)
meant to run on a distributed system.
The basic unit of a distributed system is a *node*.
Each node locally executes (its portion of) an algorithm,
sending and receiving *messages* to and from other nodes over a communication network.

MirBFT models a node of such a distributed system and presents the consumer (the programmer using MirBFT)
with a [*Node*](/node.go) abstraction.
The Node receives *requests*, processes them (while coordinating with other Nodes using the distributed protocol),
and eventually delivers them to a (consumer-defined) *application*.
Fundamentally, MirBFT's purpose can be summed up simply as receiving requests from the consumer
and delivering them to an application as prescribed by some protocol.
Te ISS protocol, for example, being a total order broadcast protocol,
guarantees that all requests received by the nodes will be delivered to the application in the same order.

Note that the application need not necessarily be an end-user application -
any program using MirBFT is, an application from MirBFT's point of view.
While the application logic is (except for the included sample demos) always expected to
be provided by the MirBFT consumer, this need not be the case for the protocol.
While the consumer is free to provide a custom protocol component,
MirBFT will provide out-of-the-box implementations of different distributed protocols for the consumer to select
(the first of them being ISS).

We now describe how the above (providing an application, selecting a protocol, etc.) works in practice.
This is where MirBFT's modular design comes into play.
The Node abstraction mentioned above is implemented as a Go struct that contains multiple *modules*.
In short, when instantiating a Node, the consumer of MirBFT provides implementations of these modules to MirBFT.
For example, instantiating a node might look as follows:

```go
    // Example Node instantiation adapted from samples/chat-demo/main.go
    node, err := mirbft.NewNode(/*some more arguments*/ &modules.Modules{
        Net:      grpcNetworking,
        // ...
        Protocol: issProtocol,
        App:      NewChatApp(reqStore),
        Crypto:   ecdsaCrypto,
    })
```

Here the consumer provides modules for networking
(implements sending and receiving messages over the network, in this case using gRPC),
the protocol logic (using the ISS protocol), the application (implementing the logic of a chat app), and a cryptographic
module (able to produce and verify digital signatures using the ECDSA algorithm).
There are more modules a Node is using, some of them always have to be provided,
some can be left out and MirBFT will fall back to default built-in implementations.
Some modules, even though they always need to be explicitly provided at Node instantiation,
are part of MirBFT and can themselves be instantiated easily using MirBFT library functions.

Inside the Node, the modules interact using *Events*.
Each module independently consumes, processes, and outputs Events.
This approach bears resemblance to the [actor model](https://en.wikipedia.org/wiki/Actor_model),
where Events exchanged between modules correspond to messages exchanged between actors.

The Node implements an event loop, where all Events created by modules are stored in a buffer and, based on their types,
distributed to their corresponding modules for processing.
For example, when the networking module receives a protocol message over the network,
it generates a `MessageReceived` Event (containing the received message)
that the Node implementation routes to the protocol module, which processes the message,
potentially outputting `SendMessage` Events that the Node implementation routes back to the networking module.

The architecture described above enables a powerful debugging approach.
All Events in the event loop can, in debug mode, be recorded, inspected, or even replayed to the Node
using a debugging interface.

The high-level architecture of a Node is depicted in the figure below.
For more details, see the [module interfaces](/pkg/modules)
and a more detailed description of each module in the [Documentation](/docs).
![High-level architecture of a MirBFT Node](/docs/images/high-level-architecture.png)

### Relation to the Mir-BFT algorithm

The term Mir-BFT was introduced as a name for a scalable atomic broadcast algorithm -
the [Mir-BFT algorithm](https://arxiv.org/abs/1906.05552).
The MirBFT library initially started as an implementation of that (old) algorithm - thus the shared name -
but the algorithm implemented within the library
has since been replaced by its modular and superior successor, [ISS](/pkg/iss).
Thus, we refer to the library / framework as MirBFT, and to the algorithm it currently implements as ISS.
Since MirBFT is designed to be modular and versatile, ISS is just one (the first) of the algorithms implemented in ISS.

## Current Status

This library is in development and not usable yet.
This document describes what the library *should become* rather than what it *currently is*.
This document itself is more than likely to still change.
You are more than welcome to contribute to accelerating the development of the MirBFT library.
Have a look at the [Contributions section](#contributing) if you want to help out!

[![Build Status](https://github.com/hyperledger-labs/mirbft/actions/workflows/test.yml/badge.svg)](https://github.com/hyperledger-labs/mirbft/actions)
[![GoDoc](https://godoc.org/github.com/hyperledger-labs/mirbft?status.svg)](https://godoc.org/github.com/hyperledger-labs/mirbft)

## Compiling and running tests

The MirBFT library relies on Protocol Buffers.
The `protoc` compiler and the corresponding Go plugin need to be installed.
Moreover, some dependencies require `gcc` to be installed as well.
On Ubuntu Linux, those can be installed using

```shell
sudo snap install --classic go
sudo snap install --classic protobuf
sudo apt install gcc
```

Once instaled, the Protocol Buffer files need to be generated by executing

```shell
go generate ./protos
```

in the `mirbft` root repository directory.
This command also has to be executed each time the `.proto` files in the `protos` folder change.

Now the tests can be run by executing

```shell
go test
```

The dependencies should be downloaded and installed automatically.

## Documentation

For a description of the design and inner workings of the library, see [MirBFT Library Architecture](/docs).

For a small demo application, see [/samples/chat-demo](/samples/chat-demo)

## Contributing

**Contributions are more than welcome!**

If you want to contribute, have a look at our [Contributor's guide](CONTRIBUTING.md)
and at the open [issues](https://github.com/hyperledger-labs/mirbft/issues).
If you have any questions (specific or general),
do not hesitate to drop an email to the active maintainer(s).

### Public Bi-Weekly Community Call

There is a public community call once every two weeks.
The current status, any issues, future plans, and anything relevant to the project will be discussed.
Whether you have any questions that you want to ask or you have a proposal to discuss, or whether you just want to listen in, feel free to join!

Meeting information:
- Time: Tuesdays in the even weeks (wrt. week number in the calendar year), between 09:00 GMT and 09:40 GMT
- Join link: [https://us05web.zoom.us/j/82410342226?pwd=bmxnOXBxZnRUN2dyTGVWQk16RW9JUT09](https://us05web.zoom.us/j/82410342226?pwd=bmxnOXBxZnRUN2dyTGVWQk16RW9JUT09)
- Meeting ID: 824 1034 2226
- Passcode: HQG5z5
- Upcoming calls:
  * Mar 22nd 2022
  * Apr 5th 2022 -- CANCELED
  * Apr 19th 2022

## Research prototypes

The [research branch](https://github.com/hyperledger-labs/mirbft/tree/research) contains code developed independently
as a research prototype of the (old) Mir BFT protocol and was used to produce experimental results
for the (old) Mir BFT [research paper](https://arxiv.org/abs/1906.05552).

The [research-iss branch](https://github.com/hyperledger-labs/mirbft/tree/research-iss) contains code developed
independently as a research prototype of the ISS protocol and was used to produce experimental results for the
EuroSys22 research paper.

## Summary of references

- [Public Discord channel](https://discord.gg/7aQbvZUT6V)
- [Hyperledger Labs page](https://labs.hyperledger.org/labs/mir-bft.html)
- Paper describing the algorithm: [Extended version](https://arxiv.org/abs/2203.05681)
- [Original PBFT paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/p398-castro-bft-tocs.pdf)

## Active maintainer(s)

- [Matej Pavlovic](https://github.com/matejpavlovic) (matopavlovic@gmail.com)

## Initial committers

- [Jason Yellick](https://github.com/jyellick)
- [Matej Pavlovic](https://github.com/matejpavlovic)
- [Chrysoula Stathakopoulou](https://github.com/stchrysa)
- [Marko Vukolic](https://github.com/vukolic)

## Sponsor

[Angelo de Caro](https://github.com/adecaro) (adc@zurich.ibm.com).

## License

The MirBFT library source code is made available under the Apache License, version 2.0 (Apache-2.0), located in the
[LICENSE file](LICENSE).

## Acknowledgments

This work has been supported in part by the European Union's Horizon 2020 research and innovation programme under grant agreement No. 780477 PRIViLEDGE.
