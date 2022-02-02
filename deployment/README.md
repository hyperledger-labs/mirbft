# Deploying ISS

This document first describes the scripts that facilitate a deployment on the IBM cloud.

Next, it describes how to run experiments on IBM cloud.

Finally, it describes how to export plots from the experimental results.

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

### Visualizing the results

For each set of experiments, after it is completed, the result summary can be found under `deployment/deployment-data/cloud-xxxx/experiment-output/result-summary.csv` (replace with `remote-xxxx` for a remote deployment).<br/>
For each individual experiment results are under `deployment/deployment-data/cloud-xxxx/experiment-output/yyyy`,
where `yyyy` represents the experiment number.
There are two types of calculate results.<br/>
Timeseries (`.csv` suffix) and aggregate (average) values (`.val` suffix).<br/>


We provide two simple `Python` scripts for visualizing experimental results.

### Timeseries
The command below plots the commits from all the nodes over time.<br/>
It saves the plot in `plot.png`.<br/>
It aggregates every 50 ms data points to smoothen the plot.<br/>
```
python3 scripts/analyze/plot-hist.py plot.png sampling - - deployment-data/cloud-xxxx/experiment-output/yyyy/timeline-commit-all.csv
```

### x-y plots
Aggregate results can be plotted in a `x-y` diagram of two chosen dimentions.
The input is now the result summary document and the plot is generated for all the experiments in the set.
For example the command below creates a latency-throughput plot for all the experiments in the experiment set.
```
python3 scripts/analyze/plot-xy.py deployment-data/cloud-xxxx/result-summary.csv target-throughput throughput-trunc latency-avg-trunc
``` 

Both commands are ran from the deployment directory.

## Using TLS

The communication among peers and between clients and peers can be configured to use TLS encryption
using options in [../config/config.yml](../config/config.yml).
Keys and certificates are currently located / generated in [tls-data/](tls-data)
and the executables are expected to be run from the `deployment` directory,
unless the path to the key and certificate files is adjusted. 

The keys and certificates can be generated by running [tls-data/generate.sh](tls-data/generate.sh) without arguments.