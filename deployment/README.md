# Deploying ISS

This document first describes the scripts that facilitate a deployment on the IBM cloud.

Next, it describes how to run experiments on IBM cloud.

Next it provides low level details on how to perform an experiment on an existing cloud setup, without depending on IBM cloud cli. 

Finally it describes how to export plots from the experimental results.

**All the following commands should be run with `mirbft/deployment` as working directory.**
## IBM cloud setup

Scripts to install and setup IBM Cloud CLI are under `scripts/setup` directory: 

### Installation
Run `./scripts/setup/install.sh` 

Installs the required packages and the IBM Cloud CLI tools.<br/>
Needs to be run only once on a machine.<br/>
Only works with Ubuntu Linux.<br/>

### Initialization

Run `./scripts/setup/init.sh`
Initializes the environment for the deployment scripts to be used by logging into to the IBM Cloud.<br/>
Needs to be run once at the start of the session.<br/>
The script expects an IBM Cloud API key file named `ibmcloud-api-key`.<br/>
If such key does not exists, the script gives instructions on how to get and register one.<br/>
The script also creates a public - private ssh key pair named `ibmcloud-ssh-key` and `ibmcloud-ssh-key.pub` which will be used for the remote deployment.
The ssh public key should be uploaded to IBM cloud so that it is an authorized key for each generated virtual machine.


## ISS Deployment
In a nutshell, to run a set of experiments one has to perform 3 steps:
1. Edit the configuration generation script to describe all desired experiments.
2. Run the deployment script `deploy.sh` with the correct arguments

### deploy.sh

Script used to deploy and experiments.<br/>
Creates a new deployment directory under `deployment-data` containing the configuration for one or more experiments and runs the experiments on IBM cloud.<br/>
In detail:
* It sets up a new deployment of virtual machines on IBM cloud or uses an existing deployment (see details below).
* The deployment consists of a master machine and a set of peer (protocol nodes) and client machines.
* The number, locations and system requirements of the virtual machines is defined in a configuration generation script which is provided as an argument.
* It runs a set of experiments according to the configuration generation script.
* It fetches to the master logs of the peers and clients per experiment.
* It analyzes on the master the the experiments based on the logs.
* It compresses the raw logs and fetches to the local machine the compressed logs and the results of the performance analysis per experiment.
* It summarized the results in a `summary.csv` file.

The deployment directory is named after the deployment type: `cloud-xxxx` or `remote-xxxx`, where `xxxx` is a monotonically increasing deployment number automatically assigned to the deployment based on existing contents of the `deployment-data` directory.

Usage:

`./deploy.sh [options] deployment-type deployment-configurations [exp-id-offset]`

**options:**

`-i`: Init only deployment. Only the the configuration for the experiments of this deployment will be generated.


**deployment-type:**
1. `cloud`: creates new cloud instances in IBM Cloud for master and slave and deploys, if not init only deployment, the experiments. 
2. `remote path-to-cloud-instance-info`:  deploys the experiment configurations on existing IBM Cloud instances specified in a `cloud-instance-info` file. Such a file is generated for each new cloud deployment and can be found under the corresponding deployment directory (e.g., `deployment-data/cloud-0000/cloud-instance-info`)

**deployment-configurations:**
1. `new path-to-config-gen-script`: Creates a new set of experiment configurations based on the specified `config-gen-script` script. 
2. `deployment-data`: Uses the configuration in the specified `deployment-data` directory. 

**exp-id-offset**: the offset from which the numbering of the executed experiments starts. If not defined the default value is `0`. 

*Example 1*:  the command below start a new cloud setup and runs the experiments described in `scripts/experiment-configuration/generate-config.sh` :
```
./deploy.sh cloud new scripts/experiment-configuration/generate-config.sh
```

*Example 2*:  the command below runs the experiments described in `scripts/experiment-configuration/generate-config.sh` on the remote deployment described in the file  `deployment-data/cloud-0000/cloud-instance-info`:
```
./deploy.sh -i remote deployment-data/cloud-0000/cloud-instance-info new scripts/experiment-configuration/generate-config.sh
```

### Cancelling the remote deployment
After completing all the experiments, the remote machines can be easily cancelled by running:<br/>
`scripts/cancel-cloud-intances.sh  tag`

The `tag` here can take the following values:
* `__all__`: Destroys **all** the virtual machines on IBM cloud. **Potentially also machines running other experiments!!!**.
* `peer`: Destroys all machines with the tag *peer*.
* `clients1, clients16, or clients32`: Destroys all client machines with the corresponding tag. Client tags are defined in the experiment configuration file.
* ` path-to-cloud-instance-info`: Destroys all machines listed in the corresponding `cloud-instance-info` file.

### Configuration generation script ###
This script generates the configuration of all the experiments for a deployment. 
The script describes sets of parameters for all the experiments the deployment script will permorm.
A sample configuration generation script is found in: `scripts/experiment-configuration/generate-config.sh`.

<!--- 
### ic-deploy-instances.sh

Deploys a the application on a (group of) virtual machine(s) with a specific configuration (machine type, region, ...)
Can either be run directly (see script itself for usage), or used in ic-deploy.sh for more automated deployments.
It uses the files
  user-script-master.sh.template
  user-script-slave.sh.template
to generate
  user-script-master-*.sh
  user-script-slave-*.sh
which, in turn, are used as user scripts to be executed on the instantiated virtual machines after boot.
Requires a private SSH key (currently called ibmcloud-ssh-key) that is accepted by the virtual machines to be
present in the directory. The corresponding public key must also be registered with Github as a deploy key, as
it is used by the scripts to clone the code repository.

### ic-launch-instance.sh

Deploys a single instance of a virtual machine and initializes it.
Used by ic-deploy.sh and not meant to be run directly.

### master-commands.cmd

Example master commands to use with the example ic-deploy.sh script.
Runs 4 peers and 1 client, kills the experiment after a short time and
copies the logs over to the master machine.

### local-deploy.sh

Local deployment script, i.e. a script for a deployment on the local machine.
This script deploys a single master and any number of groups of slaves.
Each group of slaves is specified on the command line as a pair of `ni` `tagi`,
denoting, respectively, the number of slaves in the group and their tag.
The launching of the actual application is performed by the master, which reads
commands specified in a separate file (given to this script as the first argument).

**ATTENTION**: The log file names produced for slaves contain sequence numbers
corresponding to the order in which these slave processes have been started.
These are **NOT** slave IDs (assigned by the master) and **NOT** peer or client IDs
(or whatever IDs the application started by the master commands uses).

To run an example experiment with 4 peers and 2 clients (for which the `master-commands-local.cmd` is made), run

```shell script
./local-deploy.sh master-commands-local.cmd 4 peer 2 client
```

### master-commands-local.cmd

Example master commands for local deployment of 4 peers and 1 client.
Assumes that all nodes are running on the local machine and the master port is 9999.
When running the master using this set of commands, the peers must be deployed on the local machine
(the `local-deploy.sh` script can be used for that).
--->

## Master-Slave architecture
The deployment uses (usually one) master server and multiple slaves.
Each machine is running a slave program that connects to the master.
One only should need to interact with the master server
and never log in to the slave machines.

The master server has 2 main functions (see [protobufs/discovery.proto](../protobufs/discovery.proto)):
1. Orchestrate the slaves and remotely execute programs on the slave machines (`nextCommand` RPC)
2. Act as a discovery service (rendezvous server) in a distributed system (`registerPeer` and `registerClient` RPCs)

Orchestrating slaves works as follows.
When the slaves connect to the master,
they wait for the master to tell them what to do (by invoking the master's `nextCommand` RPC).
The master is reading user commands
(from a file, as command line arguments, or, interactively, from standard input)
that drive the experiment and controls the slaves correspondingly
(by remotely executing processes on the slave machines or sending signals to those processes).

To enable different roles of different machines during an experiment,
each slave has a string label called tag.
Slaves can be controlled by the master selectively using these tags.
Each peer also has a unique numeric ID assigned by the master when the slave first connects.

The master also runs a discovery service.
Peers in a distributed system can invoke the `registerPeer` RPC
that will block until the master receives a pre-configured number of such invocations.
The master then generates a message with the identities of all callers and responds with this message to everybody.
The response message also contains a numeric ID (different for each caller) assigned by the master to the caller.

The RPC `registerClient` serves a similar purpose.
The master also generates a new numeric ID for each request
(the IDs of peers and clients are assumed to be from different namespaces and thus might overlap)
and sends this ID to the client along with all the peer identities generated through invocations of `registerPeer`.
If not enough peers registered yet, `registerClient` blocks until enough peers register.

## Commands for the master server

### Command input

Commands can be given to the master server in 3 different ways:
- As command line arguments
- In a file
- Interactively via standard input

#### Command line arguments

In general the master server executes commands given as arguments on the command line.
When launching the master server, the first argument is the port to listen on (to which the slaves connect)
and all other arguments are treated as commands.
For example,
```shell script
discoverymaster 9999 wait for slaves peer 4 exec-start peer peer__id__.log "echo Hello World!" exec-wait peer sync peer stop peer
```
starts a master server, listening on port 9999 for slave connections, that:
1. Waits until 4 slaves with the tag "peer" connect
2. Runs the command "echo Hello World!" at all slaves tagged "peer",
   redirecting the command's stdout and stderr to the file `peer__id__.log`,
   where `__id__` is replaced by the slave ID.
3. Makes slaves tagged "peer" wait until the previously executed command finishes
4. Synchronizes with all slaves tagged "peer"
5. Stops the slave process for all slaves tagged "peer"

#### Commands in a file

A special command that can be given to the server as a command line argument is `file <command_file>`,
making the master read commands from a file almost the same way as if the commands were given as arguments
(with the exception of the `exec-start` command, see its description below).
For example, if run as
```shell script
discoverymaster 9999 file command-file.cmd
```
the server will read commands from `command-file.cmd`.
A command file contain commands separated by any amount of white space (newlines, spaces, tabs).
Each line starting with a hash `#`, optionally preceded by any amount of white space,
is treated as a comment and ignored.

#### Interactive command input

A special command that can be given to the server as a command line argument is a simple dash `-`,
making the master read commands from the standard input file.
All rules for reading commands from a normal file apply,
except that a special command `done` stops processing the standard input.
To make the master server read commands (possibly interactively) from the standard input, run it as
```shell script
discoverymaster 9999 -
```

#### Combining command inputs

Different ways of providing commands to the master server can be combined.
For example,
```shell script
discoverymaster 9999 wait for slaves peer 4 - file command-file.cmd peer stop peer
```
makes the master server:
1. Wait for 4 slaves with the tag "peer"
2. Start reading commands from standard input until it reads a special "done" command
3. Execute all commands found in the file `command-file.cmd`
4. Stop all slaves tagged "peer"

### Tags and slave IDs

Each slave process has a string _tag_ (specified on the command line at startup)
and a unique integer slave _ID_ (assigned by the master server when the slave connects).

For all master commands that involve a slave tag
(such that only slaves with a specific tag should be affected by the command),
the wildcard `__all__` that matches all tags can be used.

For the `exec-start` command, the wildcard `__id__` is replaced, at each slave,
by the corresponding slave ID in the arguments and the output file name.


### Descriptions of individual commands

#### `discover-reset` _numpeers_

Reset the discovery server state and configure the server to wait for _numpeers_ invocations to `registerPeer`
before responding to those invocations.
This command must be executed before the server is ready to be used for discovery by peers and clients.

Example:

```
discover-reset 4
```

#### `discover-wait`

Wait until all peers (as specified in a previous call to `discover-reset`) have invoked the `registerPeer` RPC.
Only then proceed to the next command.

Example:
```
discover-wait
```

#### `exec-signal` _tag_

Send a signal to the process executing the program started previously using exec-start.
The signal is assumed to terminate the process and `exec-wait` will not have any effect.
Currently supported signals are
- SIGHUP
- SIGINT
- SIGKILL
- SIGUSR1
- SIGUSR2
- SIGTERM

Example:
```
exec-signal peer SIGINT
```

#### `exec-start` _tag_ _outfilename_ _command_

Launch a program on all slaves tagged with _tag_ (but do not wait until the program finishes).
The program is represented as a single string provided as _command_, e.g. "echo 'Foobar'".
The program is started by the slave process from within the go process using exec,
so no fancy shell constructs like pipes or output redirection work.

ATTENTION: The arguments also don't support spaces (in which case they are split in two separate arguments).

The output of _command_ (both stderr and stdout)
will be stored in _outfilename_ on the slave machine.

If given as a command line parameter, the _command_ must be enclosed in quotes to be treated as a single argument.
If used in a file (or interactively), everything between _outfilename_ and the end of the line is considered
as _command_ and no quotes must be used.

Example (as given in a file):
```
exec-start peer peer__id__.log echo Hello World!
```
Example (as given in a command line argument):
```
exec-start peer peer__id__.log "echo Hello World!"
```

#### `exec-wait` _tag_

Wait until the last executed program (started using `exec-start`) finishes at slaves tagged with _tag_.

Note that this makes the slave wait until the command finishes before asking the master for the next command.
The master, however, may continue processing commands immediately.
To synchronize with the slave, see the `sync` command.

Example:
```
exec-wait peer
```

#### `stop` _tag_

Stop and shut down all slaves tagged with _tag_.

Example:
```
stop __all__
```

#### `sync` _tag_

Sends a dummy "noop" command to all slaves tagged with _tag_ and waits until those slaves request the next command.
Useful for flushing the command pipeline.

Example:

```
sync peer
```
#### `wait for slaves` _tag_ _numslaves_

Wait until _numslaves_ slaves tagged with _tag_ connect to the master.
Unlike `exec-wait`, which makes slaves wait while the master continues processing commands,
this command actually makes the master itself wait.

Example:
```
wait for slaves peer 4
```

#### `wait for ` _time_

Make the master wait for a duration of _time_ before processing the next command.
_time_ can be any expression parseable by Go's `time.ParseDuration()` function.

Example:
```
wait for 1m30s
```


## Using TLS

The communication among peers and between clients and peers can be configured to use TLS encryption
using options in [../config/config.yml](../config/config.yml).
Keys and certificates are currently located / generated in [tls-data/](tls-data)
and the executables are expected to be run from the `deployment` directory,
unless the path to the key and certificate files is adjusted. 

The keys and certificates can be generated by running [tls-data/generate.sh](tls-data/generate.sh) without arguments.