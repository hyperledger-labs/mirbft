# Insanely Scalable State-Machine Replication (ISS)

This is a modular framework for implementing, deploying and testing a distributed ordering service.
The main task of such a service is maintaining a totally ordered _Log_ of client _Requests_.
This implementation uses multiple instances of an ordering protocol and multiplexes their outputs into the final _Log_.
The ordering protocol instances running on each peer are orchestrated by a _Manager_ module that decides which instance
is responsible for which part of the _Log_, when to execute a checkpoint protocol and which client requests are to be
ordered by which ordering instance. The decisions of the _Manager_ must be consistent across all peers.

The _Log_ is a sequence of _Entries_. Each _Entry_ has a _sequence number_ (_SN_) defining its position in the _Log_,
and contains a _Batch_ of _Requests_.
The _Log_ is logically partitioned into _Segments_ - parts of the _Log_ attributed to a single instance of an ordering
protocol. It is the _Manager_'s task to create these _Segments_ and to instantiate the ordering protocol for each
created _Segment_.

The set of all possible client _Requests_ is partitioned (based on their hashes) into subsets called _Buckets_.
The manager assigns a _Bucket_ to each _Segment_ it creates. The ordering protocol instance ordering that _Segment_
only creates batches of _Requests_ using the assigned _Bucket_. It is the _Manager_'s task to create _Segments_ and
assign _Buckets_ in a way ensuring that no two _Segments_ that are being ordered concurrently are assigned the same
_Bucket_. This is required to prevent request duplication.

The _Manager_ observes the _Log_ and creates new _Segments_ as the _Log_ fills up.
When the _Manager_ creates a new _Segment_, it triggers the _Orderer_ that orders the _Segment_.
Ordering a _Segment_ means committing new _Entries_ with the _SNs_ of that _Segment_.
Periodically, the _Manager_ triggers the _Checkpointer_ to create checkpoints of the _Log_.
The _Manager_ observes the created checkpoints and issues new _Segments_ as the checkpoints advance, respecting the
_watermark window_.


## Installation
### Cloning the repository
Create a GOPATH directory and make sure you are the owner of it:

`sudo mkdir -p /opt/gopath/`

`sudo chown -R $user:$group  /opt/gopath/`

where `$user` and `$group` your user and group respectively.

Create a directory to clone the repository into:

`mkdir -p /opt/gopath/src/github.com/hyperledger-labs/`

Clone this repository unter the directory you created:

`cd /opt/gopath/src/github.com/hyperledger-labs/`

`git clone https://github.com/hyperledger-labs/mirbft.git`

Checkout the`research-iss` branch.

### Installing Dependencies
With `/opt/gopath/src/github.com/hyperledger-labs/mirbft` as working directory, go to the deployment directory:

`cd deployment`

Configure the `user` and `group` in `vars.sh`

To install Golang and requirements: 

`source scripts/install-local.sh`

**NOTE**: The `install-local.sh` script, among other dependencies, installs `Go` in the home directory, sets GOPATH to `/opt/gopath/bin/` and edits `~/.bashrc`.

The default path to the repository is set to: `/opt/gopath/src/github.com/hyperledger-labs/mirbft/`.


### ISS Installation
The `run-protoc.sh` script needs to be run from the project root directory (i.e. `mirbft`) before compiling the Go
files. 

**IMPORTANT**: go modules are not supported. Disable with the command: `export GO111MODULE=off` before installation.

Compile and install the go code by running `go install ./...` from the project root directory.


## Deployment & Permformance Metrics
Detailed instructions can be found  [here](https://github.com/hyperledger-labs/mirbft/tree/research-iss/deployment).


## Glossary of terms 

### Batch
An ordered sequence of client _Requests_. All _Requests_ in a _Batch_ must belong to the same _Bucket_. The _Batch_ is
defined in the `request` package.

### Bucket
A subset of all possible client _Requests_. Each _Request_ maps to exactly one _Bucket_ (mapping is based on the
_Request_'s hash). The _Manager_ assigns one _Bucket_ to each _Segment_ and the _Orderer_ of the _Segment_ only uses
_Requests_ from the assigned _Bucket_ to propose new _Batches_. The _Bucket_ is defined in the `request` package.

### Checkpointer
Module responsible for creating checkpoints of the log. The _Checkpointer_ listens to the _Manager_, which notifies the
_Checkpointer_ about each _SN_ at which a checkpoint should occur. The _Checkpointer_ triggers a separate instance of
the checkpointing protocol for each such _SN_. When a checkpoint is stable, the _Checkpointer_ submits it to the _Log_.
Defined in the `checkpointer` package.

### Entry
One element of the _Log_. It contains a _sequence number_ (_SN_) defining its position in the _Log_ and a _Batch_ of
_Requests_. Defined in the `log` package.

### Log
A sequence of _Entries_ replicated by the peers. The `log` package implements this abstraction and all related
functionality.

### Manager
Module orchestrating all components of the ordering service implementation. The _Manager_ observes the _Log_, issues
_Segments_ and triggers the _Checkpointer_. It maintains a _watermark window_ into which all the issued _Segments_ must
fall. The decisions of the _Manager_ must be consistent across all peers. Defined in the `manager` package.

### Orderer
Module implementing the actual ordering of _Batches_, i.e., committing new _Entries_ to the _Log_.
The _Orderer_ listens to the _Manager_ for new _Segments_. Whenever the _Manager_ issues a new _Segment_, the _Orderer_
creates a new instance of the ordering protocol that proposes and agrees on _Request_ _Batches_, one for each _SN_ that
is part of the _Segment_. When a _Batch_ has been agreed upon for a particular _SN_, the _Orderer_ commits the
(_SN_, _Batch_) pair as an _Entry_ to the _Log_. Defined in the `orderer` package.

### Request
Opaque client data. Each _Request_ deterministically maps to a _Bucket_. Defined in the `request` package.

### Segment
Part of the _Log_ ,i.e., a subset of (not necessarily contiguous) _SNs_, ordered independently by an _Orderer_.
Segments are disjoint. No _SN_ can appear in more than one single _Segment_. The _Segment_ data structure (defined in
the `manager` package) completely describes an instance of the ordering protocol: the _SNs_ it is responsible for, the
sequence of leaders, the set of followers, the assigned _Bucket_, as well as information on when it is safe to start
ordering it.

### Sequence number (SN)
32-bit integer referencing a particilar position of the _Log_.

### Watermark window
A range of _SNs_ for which _Entries_ can be proposed. The _watermark window_ starts at the last stable checkpoint and
has a certain length that is a system parameter.

