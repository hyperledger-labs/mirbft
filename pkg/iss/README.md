# Insanely Scalable SMR (ISS)

ISS is the first modular algorithm to make leader-driven total order broadcast scale in a robust way.
At its interface, ISS is a classic
[state machine replication (SMR)](https://en.wikipedia.org/wiki/State_machine_replication) system
that establishes a total order of client requests with typical liveness and safety properties,
applicable to any replicated service, such as resilient databases or a blockchain ordering layer.
It is a further development and a successor of the [Mir-BFT protocol](https://arxiv.org/abs/1906.05552)
(not to be confused with the MirBFT library using which ISS is implemented,
see the [description of MirBFT](/README.md#relation-to-the-mir-bft-algorithm)).


ISS achieves scalability without requiring a primary node to periodically decide on the protocol configuration.
It multiplexes multiple instances of a leader-driven consensus protocol
which operate concurrently and (almost) independently.
We abstract away the logic of the used consensus protocol and only define an interface -
that we call "Sequenced Broadcast" (SB) - that such a consensus protocol must use to interact with ISS.

ISS maintains a *contiguous log* of (batches of) client requests at each node.
Each position in the log corresponds to a unique *sequence number*
and ISS agrees on the assignment of a unique request batch to each sequence number.
Our goal is to introduce as much parallelism as possible in assigning batches to sequence numbers
while *avoiding request duplication*, i.e., assigning the same request to more than one sequence number.
To this end, ISS subdivides the log into non-overlapping *segments*.
Each segment, representing a subset of the log's sequence numbers,
corresponds to an independent consensus protocol instance
that has its own leader and executes concurrently with other instances.

To prevent the leaders of two different segments from concurrently proposing the same request,
and thus wasting resources, while also preventing malicious leaders from censoring
(i.e., not proposing) certain requests,
we adopt and generalize the partitioning of the request space introduced by Mir-BFT.
At any point in time, ISS assigns a different subset of client requests (that we call a _bucket_) to each segment.
ISS periodically changes this assignment,
such that each request is guaranteed to eventually be assigned to a segment with a correct leader.

The figure below shows the high-level architecture of the ISS protocol.
![High-level architecture of the ISS protocol](/docs/images/high-level-architecture-iss.png)