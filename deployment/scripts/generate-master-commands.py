import os.path
import sys
from collections import defaultdict
import fileinput

CLIENT_TIMEOUT = 480000 # In milliseconds
SIGNAL_DELAY = "5s"
STOP_SLAVES_DELAY = "3s"
SCP_RETRY_COUNT = "10"
MASTER_CONFIG_DIR = "experiment-config"
MASTER_EXP_DIR="current-deployment-data"
SLAVE_CONFIG_FILE = "config/config.yml"
LOCAL_MASTER_STATUS_FILE = "master-status"
LOCAL_IP_ADDRESS = "127.0.0.1"
LOCAL_MASTER_PORT = "9999"


lastFinished = -1
deploymentSchedule = []
numSlaves = defaultdict(int)
skipAllExisting = False


def output(data):
    print(data, file=outFile)


def waitForSlaves(slaves):
    output("# Wait for slaves.")
    for s in slaves:
        output("wait for slaves {0} {1}".format(s, numSlaves[s]))
    output("")


def createLogDir(expID):
    output("# Create log directory.")
    output("exec-start __all__ /dev/null mkdir -p experiment-output/{0}/slave-__id__".format(expID))
    output("exec-wait __all__ 2000")
    output("")


def createLocalLogDir(expID):
    output("# Create log directory.")
    output("exec-start __all__ /dev/null mkdir -p experiment-output/{0}/slave-__id__/config".format(expID))
    output("exec-wait __all__ 2000")
    output("")


def pushConfigFiles(expID, slaves):
    output("# Push config files.")
    for s, configFile in slaves.items():
        output(
            "exec-start {0} scp-output-{1}-config.log stubborn-scp.sh {5} -i $ssh_key_file $own_public_ip:{2}/{3} {4}"
            "".format(s, expID, MASTER_CONFIG_DIR, configFile, SLAVE_CONFIG_FILE, SCP_RETRY_COUNT)
        )
        output("exec-wait {0} 60000 "
               "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Could not fetch config; "
               "exec-wait {0} 2000".format(s, expID))
    for s in slaves:
        output("sync {0}".format(s))
    output("")


def pushLocalConfigFiles(expID, slaves):
    output("# Prepare config file.")
    for s, configFile in slaves.items():
        output(
            "exec-start {0} /dev/null cp {1}/{2} experiment-output/{3}/slave-__id__/{4}".format(s, local_config_dir, configFile, expID, SLAVE_CONFIG_FILE)
        )
        output("exec-wait {0} 2000".format(s))
    for s in slaves:
        output("sync {0}".format(s))
    output("")


def setBandwidth(expID, bandwidths):
    output("# Set bandwidth limits.")
    for s, bandwidth in bandwidths.items():
        if bandwidth != "0" and bandwidth != "unlimited":
            output(
                "exec-start {0} set-bandwidth-{1}.log tc qdisc add dev eth0 root tbf rate {2} burst 320kbit latency 400ms"
                "".format(s, expID, bandwidth)
            )
            output("exec-wait {0} 2000 "
                   "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Could not set bandwidth; "
                   "exec-wait {0} 2000".format(s, expID))
    for s in bandwidths:
        output("sync {0}".format(s))
    output("")


def unsetBandwidth(expID, bandwidths):
    output("# Unset bandwidth limits.")
    for s, bandwidth in bandwidths.items():
        if bandwidth != "0" and bandwidth != "unlimited":
            output(
                "exec-start {0} unset-bandwidth-{1}.log tc qdisc del dev eth0 root tbf rate {2} burst 320kbit latency 400ms"
                "".format(s, expID, bandwidth)
            )
            output("exec-wait {0} 2000 "
                   "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Could not unset bandwidth; "
                   "exec-wait {0} 2000".format(s, expID))
    for s, bandwidth in bandwidths.items():
        output("sync {0}".format(s))
    output("")


def startPeers(expID, peers):
    numPeers = 0
    for p in peers:
        numPeers += numSlaves[p]

    output("# Start peers.")
    output("discover-reset {0}".format(numPeers))
    for p in peers:
        output(
            "exec-start {0} experiment-output/{1}/slave-__id__/peer.log orderingpeer "
            "{2} $own_public_ip:$master_port __public_ip__ __private_ip__ "
            "experiment-output/{1}/slave-__id__/peer.trc experiment-output/{1}/slave-__id__/prof".format(
                p, expID, SLAVE_CONFIG_FILE))
    output("discover-wait")
    output("")



def startLocalPeers(expID, peers):
    numPeers = 0
    for p in peers:
        numPeers += numSlaves[p]

    output("# Start peers.")
    output("discover-reset {0}".format(numPeers))
    for p in peers:
        output(
            "exec-start {0} experiment-output/{1}/slave-__id__/peer.log orderingpeer "
            "experiment-output/{1}/slave-__id__/{2} {3}:{4} {3} {3} "
            "experiment-output/{1}/slave-__id__/peer.trc experiment-output/{1}/slave-__id__/prof".format(
                p, expID, SLAVE_CONFIG_FILE, LOCAL_IP_ADDRESS, LOCAL_MASTER_PORT))
    output("discover-wait")
    output("")


def runClients(expID, clients):
    output("# Run clients and wait for them to stop.")
    for c in clients:
        output(
            "exec-start {0} experiment-output/{1}/slave-__id__/clients.log orderingclient "
            "{2} $own_public_ip:$master_port experiment-output/{1}/slave-__id__/client "
            "experiment-output/{1}/slave-__id__/prof-client".format(
                c, expID, SLAVE_CONFIG_FILE))
    timeout = CLIENT_TIMEOUT
    for c in clients:
        output("exec-wait {0} {2} "
               "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Client failed or timed out; "
               "exec-wait {0} 2000".format(c, expID, timeout))
        output("sync {0}".format(c))
        timeout //= 2 # In case of problems, avoids waiting for the full timeout for all sets of clients.
                     # Not setting "timeout" directly to a very small value for all except the first set of clients
                     # avoids the situation where the first set of clients exits normally and the second set of clients,
                     # taking just slightly longer, is killed. The total timeout is still bounded by 2 * CLIENT_TIMEOUT
    output("")



def runLocalClients(expID, clients):
    output("wait for 2s")
    output("# Run clients locally and wait for them to stop.")

    for c in clients:
        output(
            "exec-start {0} experiment-output/{1}/slave-__id__/clients.log orderingclient "
            "experiment-output/{1}/slave-__id__/{2} {3}:{4} experiment-output/{1}/slave-__id__/client "
            "experiment-output/{1}/slave-__id__/prof-client".format(
                c, expID, SLAVE_CONFIG_FILE, LOCAL_IP_ADDRESS, LOCAL_MASTER_PORT))
    timeoutSet = False
    for c in clients:
        if not timeoutSet:
            timeout = CLIENT_TIMEOUT
        else:
            timeout = 1000
        output("exec-wait {0} {2} "
               "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Client failed or timed out; "
               "exec-wait {0} 2000".format(c, expID, timeout))
        output("sync {0}".format(c))
    output("")


def stopPeers(peers):
    output("# Stop peers.")
    for p in peers:
        output("exec-signal {0} SIGINT".format(p))
    output("wait for {0}".format(SIGNAL_DELAY))
    output("")


def saveConfig(expID, slaves):
    output("# Save config file.")
    for s in slaves:
        output("exec-start {0} /dev/null cp {1} experiment-output/{2}/slave-__id__".format(s, SLAVE_CONFIG_FILE,
                                                                                                 expID))
        output("exec-wait {0} 2000 "
               "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Could not log config file; "
               "exec-wait {0} 2000".format(s, expID))
    output("")


def submitLogs(expID, slaves):
    output("# Submit logs to master node")
    for s in slaves:
        output(
            "exec-start {0} /dev/null tar czf experiment-output-{1}-slave-__id__.tar.gz "
            "experiment-output/{1}/slave-__id__".format(
                s, expID))
        output("exec-wait {0} 30000 "
               "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Could not compress logs; "
               "exec-wait {0} 2000".format(s, expID))
    for s in slaves:
        output(
            "exec-start {0} scp-output-{1}-logs.log stubborn-scp.sh {2} -i $ssh_key_file "
            "experiment-output-{1}-slave-__id__.tar.gz $own_public_ip:{3}/raw-results/".format(s, expID, SCP_RETRY_COUNT, MASTER_EXP_DIR))
        output("exec-wait {0} 60000 "
               "exec-start {0} experiment-output/{1}/slave-__id__/FAILED echo Could not submit logs; "
               "exec-wait {0} 2000".format(s, expID))
    for s in slaves:
        output("sync {0}".format(s))
    output("")


def updateStatus(finishedExpID):
    output("# Update master status.")
    output("write-file $status_file {0}".format(finishedExpID))
    output("")


def updateLocalStatus(finishedExpID):
    output("# Update master status.")
    output("write-file {0} {1}".format(LOCAL_MASTER_STATUS_FILE, finishedExpID))
    output("")


def stopAll():
    output("# Stop all slaves.")
    output("stop __all__")
    output("wait for {0}".format(STOP_SLAVES_DELAY))


def writeReadyFile():
    output("write-file $ready_file READY")
    output("")

def writeLocalReadyFile():
    output("write-file master-ready READY")
    output("")


def generateCommands(expID, peers, clients):

    output("#========================================")
    output("# {0}".format(expID))
    output("#========================================")
    output("\n")

    # both peers and clients are dictionaries with slave tags as keys
    # and the assigned config file names as the corresponding values.
    # config is thus the assignment to slave tag to config file.
    config = peers.copy()
    config.update(clients)

    configFiles = {key: val[0] for key, val in config.items()}
    bandwidths = {key: val[1] for key, val in config.items()}

    # slaves are all the tags of the used slaves, both peers and clients.
    slaves = list(peers) + list(clients)

    # most of these functions only need the keys (slave tags)
    # and only pushConfigFiles() needs the values (config files) too.
    waitForSlaves(slaves)
    createLogDir(expID)
    pushConfigFiles(expID, configFiles)
    setBandwidth(expID, bandwidths)
    startPeers(expID, list(peers))
    runClients(expID, list(clients))
    stopPeers(list(peers))
    unsetBandwidth(expID, bandwidths)
    saveConfig(expID, slaves)
    submitLogs(expID, slaves)
    updateStatus(expID)

    output("")

def generateLocalCommands(expID, peers, clients):

    output("#========================================")
    output("# {0} (local)".format(expID))
    output("#========================================")
    output("\n")

    # both peers and clients are dictionaries with slave tags as keys
    # and the assigned config file names as the corresponding values.
    # config is thus the assignment to slave tag to config file.
    config = peers.copy()
    config.update(clients)

    configFiles = {key: val[0] for key, val in config.items()}
    bandwidths = {key: val[1] for key, val in config.items()}

    # slaves are all the tags of the used slaves, both peers and clients.
    slaves = list(peers) + list(clients)

    # most of these functions only need the keys (slave tags)
    # and only pushConfigFiles() needs the values (config files) too.
    waitForSlaves(slaves)
    createLocalLogDir(expID)
    pushLocalConfigFiles(expID, configFiles)
    startLocalPeers(expID, list(peers))
    runLocalClients(expID, list(clients))
    stopPeers(list(peers))
    updateLocalStatus(expID)

    output("")


def deploy(tokens):
    global defaultMachine
    global deploymentSchedule
    global numSlaves
    global lastFinished

    machine = defaultMachine

    while tokens:
        if tokens[0] == "machine:":
            machine = tokens[1]
            tokens = tokens[2:]
        else:
            if machine != "":
                n = int(tokens[0])
                tag = tokens[1]
                templateFile = machine

                numSlaves[tag] += n
                deploymentSchedule.append((lastFinished, n, tag, templateFile))
            else:
                sys.exit("ic-parse-experiment.py: deploy: must specify machine template before token '{0}'".format(tokens[0]))
            tokens = tokens[2:]


def run(expID, tokens):
    global lastFinished
    global idOffset
    global experimentIdDigits
    global skipAllExisting

    # If special value "next" is specified as experiment ID, use the next value
    if expID == "next":
        expID = ("{:0"+str(experimentIdDigits)+"d}").format(idOffset)
        idOffset += 1
    # Otherwise, set the global idOffset to continue counting from here
    else:
        experimentIdDigits = len(expID)
        idOffset = int(expID) + 1

    # Allow user to skip existing experiments or to cancel the whole process
    skip = False
    outdir = "{0}/experiment-output/{1}".format(local_exp_data, expID)

    if os.path.isdir(outdir) and skipAllExisting:
        skip = True
    elif os.path.isdir(outdir):

        # Ask the user what to do.
        sys.stderr.write("{0} already exists. (S)kip / skip (A)ll / (C)ancel? : ".format(outdir))
        sys.stderr.flush()
        answer = sys.stdin.readline().strip()
        while not answer in {"s", "S", "a", "A", "c", "C"}:
            sys.stderr.write("Please answer a, s, or c : ")
            sys.stderr.flush()
            answer = sys.stdin.readline().strip()

        # Act based on user's answer.
        if answer == "s" or answer == "S":
            skip = True
        elif answer == "a" or answer == "A":
            skip = True
            skipAllExisting = True
        elif answer == "c" or answer == "C":
            os.exit("User abort.")

    clients = {}
    peers = {}
    config = defaultConfig
    bandwidth = defaultBandwidth
    role = None

    # Process (tag, role) pairs.
    while tokens:

        if tokens[0] == "config:":
            config = tokens[1]
            tokens = tokens[2:]
        if tokens[0] == "bandwidth:":
            bandwidth = tokens[1]
            tokens = tokens[2:]
        elif tokens[0] == "peers:":
            role = peers
            tokens = tokens[1:]
        elif tokens[0] == "clients:":
            role = clients
            tokens = tokens[1:]
        else:
            if config != "" and role is not None:
                configFile = "{0}/{1}/{2}".format(local_exp_data, local_config_dir, config)
                if os.path.isfile(configFile):
                    role[tokens[0]] = (config, bandwidth)
                else:
                    sys.exit("ic-parse-experiment.py: config file not found: {0}".format(configFile))
            else:
                sys.exit("ic-parse-experiment.py: run {0}: must specify role and config before token '{1}'".format(expID, tokens[0]))
            tokens = tokens[1:]

    if not skip:
        if deplType in {"cloud", "remote"}:
            generateCommands(expID, peers, clients)
        elif deplType == "local":
            generateLocalCommands(expID, peers, clients)
        else:
            sys.exit("generate-master-commands.py: unknown deployment type: {0} only know 'local' and 'cloud'".format(deplType))

    lastFinished = expID


def printDeploymentSchedule():
    for expID, n, tag, templateFile in deploymentSchedule:
        print("{0} {1} {2} {3}".format(expID, n, tag, templateFile))


# ============================================================
# main
# ============================================================

# get deployment type ("local" or "cloud")
deplType = sys.argv[1]
if deplType not in {"local", "cloud", "remote"}:
    sys.exit("generate-master-commands.py: first argument must be one of 'local', 'cloud', and 'remote'")

# get input file name
inFileName = sys.argv[2]

# open output file
outFile = open(sys.argv[3], "w")

# Get local experiment data directories
local_exp_data = sys.argv[4]
local_config_dir = "config"

# Initialize default values
defaultConfig = ""
defaultMachine = ""
defaultBandwidth = "unlimited"

# These are the default values, will be overridden by the explicitly set IDs.
experimentIdDigits = 3
idOffset=0

# Create "ready" file to indicate that the master server is running.
# The deployment scripts wait until this file exists before starting slaves.
if deplType == "local":
    writeLocalReadyFile()
else:
    writeReadyFile()

with open(inFileName) as inFile:
    for line in inFile:

        # Skip empty lines and comments.
        if line.strip() == "" or line.strip().startswith("#"):
            continue

        tokens = line.split()

        if tokens[0] == "deploy":
            deploy(tokens[1:])
        elif tokens[0] == "run":
            run(tokens[1], tokens[2:])
        elif tokens[0] == "config:":
            defaultConfig = tokens[1]
        elif tokens[0] == "machine:":
            defaultMachine = tokens[1]
        elif tokens[0] == "bandwidth:":
            defaultBandwidth = tokens[1]
        else:
            sys.exit("ic-parse-experiment.py: Unsupported command: {0}".format(tokens[0]))


output("#========================================")
output("# Wrap up                                ")
output("#========================================")
output("")
output("# Wait for all slaves, even if they were not involved in experiments.")
waitForSlaves(numSlaves.keys())

stopAll()

printDeploymentSchedule()

outFile.close()
