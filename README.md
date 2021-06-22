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

## Performance Evaluation

### System requirements
The evaluation was run on dedicated virtual machines:
* 4-100 servers (Mir-BFT nodes) machines
* 16 client machines

Clients and servers are located on different virtual machines. 

Each machine (server, client):
* 32 vCPUs
* 32 GB memory
* 2 network interfaces (public & private) each 1 Gbps up and down
* 100 GB disk 
* OS: Ubuntu 18.04 

### Running the evaluation
The following steps need to be followed for performance evaluation:

1. Clone the repository on each machine and install the requirements with scripts under `deployment` directory.
2. Generate a certificate authority (CA) key and certificate.
3. Generate a private key and certificate  signed by CA for each server (node).
4. Copy the private key to each server (node).
5. Copy all the certificates to all servers (node).
6. Copy the CA certificate to all clients.
7. Edit the configuration file for each server and client (see details below).
8. Start all clients: `./client ../deployment/config/clientconfig/config.yml client`. A trace file is generated as `client-id.trc`.
9. Start all servers and source their output to a log file e.g.: `./server ../deployment/config/serverconfig/config.yml server &> server.log`
10. Use the performance evaluation tool to parse the log and trc files and get performance evaluation results (see details below).

Steps 1-9 can be automated with scripts in `deployment`:

* First, add information for your cloud setup `cloud-instance.info` file.
 
    Each line must have the following format:
    
    ``machine-identifier public-ip private-ip``.
    * `machine-identifier`: should have the format `server-x` or `client-x` respectively, where `x` some counter/id. 
    * If the machine has only one `ip` address use the same in both columns.

 * Edit `vars.sh` file:
    * `user`: the user of the vms
    * `group`: the group name (could be the same as the user)
    * `private_key_file`: the absolute path to a private key tha gives ssh access to `user@public-ip` for each machine.
 
 * Run `deploy.sh` to copy and run `install.sh` and `clone.sh` on each client and server machine. This installs requirements, clones the repository and installs server and client executables.
 
 * Edit parameters in `deployment/config-file-templates/server-config.yml` and `deployment/config-file-templates/server-config.yml` (see details below).

 **IMPORTANT**: Leave fields in block letters untouched, they are automatically replaced by `config-gen.sh`.
 
 * Run `config-gen.sh` to generate certificates, configuration files and copy them to server and client machines. The script has two flags:
    * `-c` or `--config-only`: generates and copies only configuration files, not certificates.
    * `-l` or `--local`: instead of copying the certificates and configuration files to a remote machine, it creates a `deployment/config` directory and copies the files there to facilitate a local deployment.
 
 **IMPORTANT**: The scripts assume all machines have the same user and the user is in the `sudo` group without password for `sudo` commands.

* Run `run.sh`. The script:
    * Kills any previously running experiment.
    * Starts servers and clients on the remote machines as defined in`cloud-instance.info`.
    * Periodically checks if the experiment has finished.
    * Creates an `experiment-output` directory and fetches the log and trace files of the experiment in it. 
 
  **IMPORTANT**: If there are already files in `experiment-output` they will be overwritten.

### Server configuration

Each server has:
 * 2 IPs: `ip-public`, `ip-private`.
    * `ip-public`: the ip used by clients to submit requests.
    * `ip-private`: the ip used for node-to-node communication.
 * and id from `0` to `N-1`, where `N` the number of servers.
 * a private key: `server.key`
 * a certificate: `server.pem`
 * 2 listening ports: `server-to-server-port`, `server-to-client-port` such that `server-to-client-port`=`server-to-server-port+2`
 
 Comments in `sampleconfig/serverconfig/config*.yml` files describe how to configure a server.
 
 Please bare in mind:
 
 * `signatureVerification`: must be true to enable client authentication
 * `sigSharding`: must be true to enable signature verification sharding (SVS). Mir by default is considered to have SVS.
 * `payloadSharding`: must be true to enable light total order broadcast (LTO) optimization.
 * `watermarkDist`: should be greater than or equal to `checkpointDist`.
 * `watermarkDist`: should be also greater than or equal to the number of nodes.
 * `bucketRotationPeriod`: should be greater than or equal to `watermarkDist`
 * `clientWatermarkDist`: should be set to a very large value to allow few clients saturate throughput. Otherwise the setup would require many client machines.

 In `self` section:
 * `listen` should always be set to `"0.0.0.0:server-to-server-port"`
 
 In `servers` section:
 * `certFiles` make sure they have the same order as the corresponding server’s `id`.
 * `addresses`:
    * make sure they have the same order as the corresponding server’s `id`. 
    * use `ip-private` addresses.
    * make sure the port number `server-to-server-port` matches the port number in the `self` section of each server.
 
### Client configuration
 Comments in `sampleconfig/serverconfig/config*.yml` files describe how to configure a server.

 In `servers` section:
 * `addresses`:
     * make sure they have the same order as the corresponding server’s `id`. 
     * use `ip-public` addresses.
     * make sure the port number `server-to-client-port` matches the port number in the `self` section of each server increased by `2`.
  
### Experiment duration:
Experiments where performed for minutes (1-2 minutes) or for for few (1-4) million client requests in total.

Client runtime can be tuned with `clientRunTime` and `requestsPerClient` parameters in client configuration.
*  `requestsPerClient`: The total number of requests a client process submits.
*  `clientRunTime`: A client process is killed after `clientRunTime` milliseconds, regardless if `requestsPerClient` requests are submitted. 

Servers must be killed manually.

### Latency-Throughput plots
 Progressively increase load from clients (i.e,. in every subsequent run increase the load from clients) by increasing `requestRate` and `parallelism` until the throughput of the system stops increasing and latency increases significantly.

### Scalability plots
For each number of nodes `N` run a Latency-Throughput plot and peak the maximum throughput.

### Experiments with faults
Configure the servers with the parameters in `Byzantine behavior` section of the configuration.
* To emulate straggler behavior set `ByzantineDelay` to a non-zero value, smaller than `epochTimeoutNsec`.
* To emulate crash faults set `ByzantineDelay` to a large value, greater than `epochTimeoutNsec`.
* To emulate censoring set `censoring` to a non-zero percentage.

### Simulating protocols  without duplication prevention
In `Byzantine behavior` section, set `byzantineDuplication` to true.

### Performance Evaluation Tool
Performance evaluation metrics *throughput* and *latency* can be calculated with the `tools/perf-eval.py` with the logs generated by the `server/server` and traces generated by the `client/client` binaries.

The script should be called as follows:
`python tools/perf-eval.py n m [server_1.out ... server_m.out] [client_001.trc ... client_m.trc] x y`
* `n`: the number of server log files
* `m`: the number of client log files
* `[server_1.out ... server_m.out]`: list of server log files
* `[client_001.trc ... client_m.trc]`: list of client trace files
* `x`: number of requests to ignore at the beginning of the experiment for throughput calculation
* `y`: number of requests to ignore at the end of the experiment for throughput calculation

The output of the script is: 
```$xslt
End to end latency: #### ms
Average request rate per client: #### r/s
Experiment duration: #### s
Throughput: #### r/s
Requests: ####
```
Moreover the script generates a file `latency.out` with latency CDF.
