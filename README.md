# MirBFT Research Prototype
The implementation for  [Mir-BFT: High-Throughput Robust BFT for Decentralized Networks
](https://arxiv.org/abs/1906.05552) paper.

**DISCLAIMER**: The deployment has been tested on machines running Ubuntu 18.04

## Local Deployment
### Dependencies

Go to the deployment directory:

`cd deployment`

Configure the `user` and `group` in `vars.sh`

To install Golang and requirements: 

`./install-local.sh`

**DISCLAIMER**: The `./install-local.sh` script, among other dependencies, installs `Go` in the home directory, sets GOPATH to `/opt/gopath/bin/` and edits `~/.profile`.

The default path to the repository is set to: `/opt/gopath/src/github.com/IBM/mirbft/`.


### Installation

Clone this repository under `/opt/gopath/src/github.com/IBM/`.

Checkout the`research` branch.

Build the protobufs with `/opt/gopath/src/github.com/IBM/mirbft` as working directory:

`./run-protoc.sh`

To compile the server:

`cd server`

`go build`

To compile the client:

`cd client`

`go build`

### Running with a sample configuration
A server sample configuration exists in `sampleconfig/serverconfig/` .

A client sample configuration exists in `sampleconfig/clientconfig/`.

#### 4 servers - 1 client
To start locally a setup with 4 server and 1 client:

On each server:

`cd server`

`./server ../sampleconfig/serverconfig/config$id.yml server$id`
where `$id` is `1 2 3 4` for each of the 4 server.

The first argument is the path to the server configuration and the second argument a name prefix for a trace file.


On the client:

`cd client`

`./client ../sampleconfig/clientconfig/4peer-config.yml client`

Again, the first argument is the path to the server configuration and the second argument a name prefix for a trace file.


#### 1 server - 1 client

On the server:

`cd server`

`./server ../sampleconfig/serverconfig/config.yml server`

On the client:

`cd client`

`./client ../sampleconfig/clientconfig/1peer-config.yml client`

**IMPORTANT**: Client processes finish according to their configuration parameters (see details below), server processes must be killed manually.

### Running with a custom configuration

Change working directory to `/opt/gopath/src/github.com/IBM/mirbft/deployment`

Edit `config-file-templates/server-config.yml` and  `config-file-templates/client-config.yml` for server, client configuration respectively (see details below).
 
**IMPORTANT**: Leave fields in block letters untouched, they are automatically replaced by `config-gen.sh`.

Run `config-gen.sh` to generate certificates and configuration files:

`config-gen.sh -l N C`
* `-l`: a flag for generating a local configuration using the loopback address as ip for servers and clients
* `N`: number of server
* `C`: number of clients

On each server:

`cd server`

`./server ../deployment/config/serverconfig/config_server$id.yml server$id` where `$id` is `1..N`.

On each client:

`cd client`

`./client ../deployment/config/clientconfig/config_client1.yml client$id` where `$id` is `1..C`.


## Remote Deployment

First, add information for your cloud setup `cloud-instance.info` file.
 
Each line must have the following format:

``machine-identifier public-ip private-ip``.
* `machine-identifier`: should have the format `server-x` or `client-x` respectively, where `x` some counter/id. 
* If the machine has only one `ip` address use the same in both columns.

Edit `vars.sh` file:
* `user`: the user of the vms
* `group`: the group name (could be the same as the user)
* `private_key_file`: the absolute path to a private key tha gives ssh access to `user@public-ip` for each machine.
 
Run `deploy.sh` to copy and run `install.sh` and `clone.sh` on each client and server machine. This installs requirements, clones the repository and installs server and client executables.
 
Edit parameters in `deployment/config-file-templates/server-config.yml` and `deployment/config-file-templates/client-config.yml` (see details below).

**IMPORTANT**: Leave fields in block letters untouched, they are automatically replaced by `config-gen.sh`.
 
Run `config-gen.sh` to generate certificates, configuration files and copy them to server and client machines. The script has two flags:
* `-c` or `--config-only`: generates and copies only configuration files, not certificates.
* `-l` or `--local`: instead of copying the certificates and configuration files to a remote machine, it creates a `deployment/config` directory and copies the files there to facilitate a local deployment.
 
**IMPORTANT**: The scripts assume all machines have the same user and the user is in the `sudo` group without password for `sudo` commands.

Run `run.sh`. The script:
* Kills any previously running experiment.
* Starts servers and clients on the remote machines as defined in`cloud-instance.info`.
* Periodically checks if the experiment has finished.
* Creates an `experiment-output` directory and fetches the log and trace files of the experiment in it. 
 
**IMPORTANT**: If there are already files in `experiment-output` they will be overwritten.

## Configuration details 
### Server configuration

Each server (node) has:
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
 
 Client runtime can be tuned with `clientRunTime` and `requestsPerClient` parameters in client configuration.
 *  `requestsPerClient`: The total number of requests a client process submits.
 *  `clientRunTime`: A client process is killed after `clientRunTime` milliseconds, regardless if `requestsPerClient` requests are submitted. 


## Performance Evaluation

### System requirements for reproducing results from [Mir-BFT](https://arxiv.org/abs/1906.05552) paper
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

The following steps need to be followed for each experiment run:

1. Add the servers and clients of the experiment  in `deployment/cloud-instance.info`.
    * If the experiment is run locally, skip this step.
2. Edit the experiment specific parameters in configuration template files under `deployment/config-file-templates`
3. Run `config-gen.sh` to generate TLS certificates and configuration files for servers and clients.
    * If you did not change the set of servers and clients run the script with `-c` option to avoid re-generating and copying the certificates.
    * If you run the experiment locally run the script with `-l`, providing also the number of servers and clients, as described above.
4. Run the experiment with `run.sh`, see details above.
5. Use the performance evaluation tool to parse the log and trc files and get performance evaluation results (see details below).
 
### Experiment duration:
Experiments where performed for minutes (1-2 minutes) or for for few (1-4) million client requests in total.

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
