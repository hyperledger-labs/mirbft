# Clients design

In Mir, we assume that clients are all explicitly enumerated and each client request is associated with a sequence number called a request number.

Naively, when a client first connects to the system, it authenticates to each node establishing its identity.  It then begins sending request number 1, request number 2, and so forth to each node.

## Motivation

The requirement for both explicitly enumerating clients and explicitly ordering client requests (request numbers) might seem burdensome (and no doubt, it does add some additional complexity for the client), but it is crucial for the operation of Mir.

Because Mir may have multiple leaders active at any given time, it's important to be able to partition the request space for a particular client into multiple buckets.  Naively, we could partition the space simply by hash of the request, but it turns out this creates more problems then it solves.  If we took each request and simply hashed it, we immediately run into problems of replay.  What if a malicious node replays this (and other previously committed) requests over and over to simulate progress while censoring other requests? We could try to add some degree of replay prevention by including a timestamp, but what if a node is byzantine and manages to delay that request beyond its timestamp validity standpoint?  It is not good enough to know if a request is for a particular bucket, we need to also be able to tell if a request is for a bucket and is valid for the current sequence watermarks.

Additionally, having client request numbers actually can ultimately simplify applications consuming Mir.  For instance, many applications end up implementing replay prevention at a higher level than the atomic broadcast because other atomic broadcast systems cannot guarantee no replay.

## Client ACKs

The MirBFT library chooses to depart from the original Mir paper in one notable way.  Rather than disseminating full transaction contents with the preprepare, this library chooses instead to disseminate the full transaction only to those replicas which have not acknowledged receipt of the original request.

This acknowledgement scheme has some significant advantages from a design perspective.  Firstly, the library never needs to concern itself with request validity directly.  If the application injects a request into the state machine, it is assumed that the application has already validated it.  If another replica attempts to forward a request, that request is only accepted if this replica already has at least a weak quorum certificate vouching for the correctness of that request.  In this way, nowhere does the library demand that the consumer inject its own validation into the hot-path of consensus.

The downside to the ack scheme is that it requires many more messages, and some additional overhead tracking those ack certificates.  It should be possible to extend the library to allow both models with minimal changes.

## Client state

The state of each client is part of the network state, and is associated with each checkpoint.  The client state includes the low watermark for that client, the width of the client's sliding request window, and any requests from that window which have committed prior to this checkpoint (expressed as a bitmask relative to the low watermark).  Additionally, the 'width' of the sliding windows consumed during the last checkpoint is recorded to ensure that replicas can use any checkpoint as a state target rather than requiring two to establish the complete window.

## Avoiding stalls and cleaning up after clients

The client tracking code consumes ticks to attempt to cleanup after clients which have disconnected inappropriately or crashed.

If a replica has a correct quorum cert for a request, but has not received this request after some number of ticks, will attempt to replicate it from the rest of the network, and then send its own ACK.  Eventually such a request should gather a strong cert and be able to be included in a preprepare.

Additionally, if a client request does not have a strong cert, the replicas with the request will periodically re-ack this request (with a backoff) in an attempt to resolve this request once adequate replicas come online.

There is a lot more discussion of client to be found in [client_tracker.go](https://github.com/hyperledger-labs/mirbft/blob/master/processor.go).
