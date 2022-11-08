# MirBFT Research Prototype
The implementation for  [Mir-BFT: High-Throughput Robust BFT for Decentralized Networks
](https://arxiv.org/abs/1906.05552) paper, (peer-reviewed Journal of Systems Research (JSYS) snapshot of the paper is available [here](https://escholarship.org/uc/item/36g369xq)).

The deployment has been tested on x86 machines running Ubuntu 20.04
The commands should be run on bash shell.

The remote deployment has been tested on IBM cloud and on AWS.

**IMPORTANT NOTES**: 
* Make sure that the network configuration allows *all* inbound and outbound network communication on *all* ports.<br />
  * For AWS, first create a security group and add an inbound network rule to enable this.
* Private IPs are not reachable by default for an AWS deployment across different regions.
Configure your VPC [accordingly](https://docs.aws.amazon.com/devicefarm/latest/developerguide/amazon-vpc-cross-region.html) or use only public network interfaces by using the machines' public IP addresses in place of the private ones in the configuration file (see details below).


## Local Deployment
Download and run the script `setup.sh` which can be found in the deployment directory:<br />
`source setup.sh` <br />

The script installs Golang 17.2 and all other dependencies.

Then it clones this repository under the path: <br />
`/opt/gopath/src/github.com/IBM/mirbft`

It checks out the `research` branch and, finally, builds the `client` and `server` executables under
`/opt/gopath/src/github.com/IBM/mirbftsever` and `/opt/gopath/src/github.com/IBM/mirbft/client`
respectively.

**NOTE**: The script installs `Go` in the home directory, sets GOPATH to `/opt/gopath/bin/` and edits `~/.bashrc`.


### Running with a sample configuration
A server sample configuration exists in `sampleconfig/serverconfig/` .

A client sample configuration exists in `sampleconfig/clientconfig/`.

#### 4 servers - 1 client
To start locally a setup with 4 server and 1 client:

On each server:

`cd /opt/gopath/src/github.com/IBM/mirbft/server`

`./server ../sampleconfig/serverconfig/config$id.yml server$id 2>&1 | tee server-$id.out`
where `$id` is `1 2 3 4` for each of the 4 server.

The first argument is the path to the server configuration and the second argument a name prefix for a trace file.

The command writes the logs of the server to a `server-$id.out` file.

On the client:

`cd /opt/gopath/src/github.com/IBM/mirbft/client`

`./client ../sampleconfig/clientconfig/4peer-config.yml client`

Again, the first argument is the path to the server configuration and the second argument a name prefix for a trace file.


#### 1 server - 1 client

On the server:

`cd /opt/gopath/src/github.com/IBM/mirbft/server`

`./server ../sampleconfig/serverconfig/config.yml server 2>&1 | tee server.out`

The command writes the logs of the server to a `server.out` file.


On the client:

`cd /opt/gopath/src/github.com/IBM/mirbft/client`

`./client ../sampleconfig/clientconfig/1peer-config.yml client`

**IMPORTANT**: Client processes finish according to their configuration parameters (see details below), server processes must be killed manually.

### Running with a custom configuration

Change working directory to `/opt/gopath/src/github.com/IBM/mirbft/deployment`

Edit `config-file-templates/server-config.yml` and  `config-file-templates/client-config.yml` for server, client configuration respectively (see details below).
 
**IMPORTANT**: Leave fields in block letters untouched, they are automatically replaced by `config-gen.sh`.

Run `config-gen.sh` to generate certificates and configuration files:

`./config-gen.sh -l N C`
* `-l`: a flag for generating a local configuration using the loopback address as ip for servers and clients
* `N`: number of server
* `C`: number of clients

On each server:

`cd /opt/gopath/src/github.com/IBM/mirbft/server`

`./server ../deployment/config/serverconfig/config_server$id.yml server$id 2>&1 | tee server-$id.out` where `$id` is `1..N`.

The command writes the logs of the server to a `server-$id.out` file.

On each client:

`cd /opt/gopath/src/github.com/IBM/mirbft/client`

`./client ../deployment/config/clientconfig/config_client$id.yml client$id` where `$id` is `1..C`.


## Remote Deployment
Change working directory to `/opt/gopath/src/github.com/IBM/mirbft/deployment`

Add information for your cloud setup `cloud-instance.info` file.
 
Each line must have the following format:

``machine-identifier public-ip private-ip``.
* `machine-identifier`: should have the format `server-x` or `client-x` respectively, where `x` some counter/id. 
* If the machine has only one `ip` address use the same in both columns.

Edit `vars.sh` file:
* `ssh_user`: the user on the remote machines
* `private_key_file`: the absolute path to a private key tha gives ssh access to `ssh_user@public-ip` for each machine.
 
Run `./deploy.sh` <br />
This copies and runs `install.sh` and `clone.sh` on each client and server machine to install requirements, clone the repository and install server and client executables.

The `deploy.sh` script has one option/flag:
`-p` or `--pull-only` does not re-install requirements, simply pulls changes from this repository and builds again the binaries

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
* Runs the performance evaluation tool (see details below) for the experiment. Note that the tool, when called in the `run.sh` script, does not truncate the data and might report latency and throughput affected by thr rump up and close down pediods.
See details below on how to truncate.

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
 
 Comments in `/opt/gopath/src/github.com/IBM/mirbft/sampleconfig/serverconfig/config*.yml` files describe how to configure a server.
 
 Please bare in mind:
 
 * `signatureVerification`: must be true to enable client authentication.
 * `sigSharding`: must be true to enable signature verification sharding (SVS). Mir by default is considered to have SVS.
 * `payloadSharding`: must be true to enable light total order broadcast (LTO) optimization.
 * `watermarkDist`: should be greater than or equal to `checkpointDist`.
 * `watermarkDist`: should be also greater than or equal to the number of nodes.
 * `bucketRotationPeriod`: should be greater than or equal to `watermarkDist`.
 * `clientWatermarkDist`: should be set to a very large value to allow few clients saturate throughput. Otherwise the setup would require many client machines.
 * `serverConnectionBuffer`: might need to be increased for large deployments to allow enough time to all servers to connect to each other.

 In `self` section:
 * `listen` should always be set to `"0.0.0.0:server-to-server-port"`
 
 In `servers` section:
 * `certFiles` make sure they have the same order as the corresponding server’s `id`.
 * `addresses`:
    * make sure they have the same order as the corresponding server’s `id`. 
    * use `ip-private` addresses.
    * make sure the port number `server-to-server-port` matches the port number in the `self` section of each server.
    
#### Emulating PBFT
To emulate PBFT we need to enforce a leaderset of size `1` and make epochs infinite.
Apply the following changes to server configuration:
* Set `maxLeaders` to `1`.
* Set `bucketRotationPeriod` to a very high number, higher than the expected number of batches for the duration of your experiment.
* Set `sigSharding` to `false` to disable the SVS optimization.
 
#### Emulating Parallel PBFT
To emulate parallel PBFT instances we need to disable the bucket redistribution.
 * Set `bucketRotationPeriod` to a very high number, higher than the expected number of batches for the duration of your experiment.
 * Set `sigSharding` to `false` to disable the SVS optimization.
 
 This configuration is meaningful only for a fault-free execution and under the assumption that all the requests delivered are unique.
 For evaluating the impact of duplicate requests, see the "Performance Evaluation" section below.

### Client configuration
 Comments in `/opt/gopath/src/github.com/IBM/mirbft/sampleconfig/clientconfig/*peer-config.yml` files describe how to configure a client.

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

1. Add the servers and clients of the experiment  in `/opt/gopath/src/github.com/IBM/mirbft/deployment/cloud-instance.info`.
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

**NOTE:** The current code version does not implement state-transfer. Therefore, some executions with crash faults get stuck.

### Simulating protocols  without duplication prevention
In `Byzantine behavior` section, set `byzantineDuplication` to true.

### Performance Evaluation Tool
Performance evaluation metrics *throughput* and *latency* can be calculated with the `/opt/gopath/src/github.com/IBM/mirbft/tools/perf-eval.py` with the logs generated by the `/opt/gopath/src/github.com/IBM/mirbft/server/server` and traces generated by the `/opt/gopath/src/github.com/IBM/mirbft/client/client` binaries.

With /opt/gopath/src/github.com/IBM/mirbft as working directory, the script should be called as follows:
`python2 tools/perf-eval.py n m [path/to/server-1.out ... path/to/server-m.out] [path/to/client-001.trc ... path/to/client-m.trc] x y`
* `n`: the number of server log file
* `m`: the number of client trace files
* `[path/to/server-1.out ... path/to/server-m.out]`: list of server *log* file paths
* `[path/to/client-001.trc ... path/to/client-m.trc]`: list of client *trace* file paths
* `x`: number of requests to ignore at the beginning of the experiment for throughput calculation
* `y`: number of requests to ignore at the end of the experiment for throughput calculation

The output of the script is: 
```$xslt
Average end to end latency: #### ms
Average request rate per client: #### r/s
Throughput: #### r/s
Requests: ####
```
Moreover the script generates a file `latency.out` with latency CDF.

**NOTE:** Unlike in the paper, the current code implementation supports responses to the clients, so as to easier automate the deployment.
The latency measured here is, therefore, end-to-end: from the client request submission until the client receives enough responses inficating that the request is delivered.
Since both data points are measured on the same machine, there is no need to synchronize server and client clocks, unlike what the paper mentions.

**IMPORTANT: The evaluation script works only with python 2**

### Plotting the data

Please see detailed instructions in `plots/README.md`
