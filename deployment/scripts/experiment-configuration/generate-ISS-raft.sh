#!/bin/bash

# Notes:
# RAFT:
#   The batch timeout should be approximately longer than an RTT in the network.
#   Otherwise, the leader does not get AppendEntryResponse messages in time and re-sends AppendEntryRequest messages.

# Machine locations:
#
# All available datacenter codes:
# ams01 ams03 che01 dal05 dal06 dal09 dal10 dal12 dal13 fra02 fra04
# fra05 hkg02 lon02 lon04 lon05 lon06 mex01 mil01 mon01 par01 sao01
# seo01 sjc01 sjc03 sjc04 sng01 syd01 syd04 syd05 tok02 tok04 tok05
# tor01 tor04 tor05 wdc01 osa21 osa22 osa23 wdc04 wdc06 wdc07
#
# LAN experiments:
# machineLocations="fra02"
#
# Mir:
# machineLocations="lon06 par01 ams03 fra05 mil01 osl01 sjc04 dal13 mex01 wdc07 tor05 mon01 che01 seo01 tok05 syd05"
# Note: The Oslo datacenter (osl01) does is not available any more.
#
# Not used by Mir:
# sao01 osa23
#
# Big dedicated machines not available in Singapore and Hong Kong:
# sng01 hkg02
#
# Problems commissioning big dedicated machines:
# tor05 dal13
#


# Deployment setup
machineType="cloud-machine-templates/dedicated-machine-32-CPUs-32GB-RAM"
machineLocations="sjc04 osa23 ams03 syd05 lon06 wdc07 che01 tok05 par01 dal10 fra05 mil01 mex01 tor01 tor04 seo01"
faultyMachineLocations="sjc04 osa23 ams03 syd05 lon06 wdc07 che01 tok05 par01 dal10 fra05 mil01 mex01 tor01 tor04 seo01"

# number of client instances per node for 1/16/32 client machines
clients1=""
clients16="16"
clients32=""
systemSizes="4 8 16 32 64 128" # Must be sorted in ascending order!
failureCounts=(0 0 0 0 0 0) # For each system size, the corresponding failure count (on top of the correct nodes)
reuseFaulty=true  # If true, both correct and faulty peers will have the same tag and will be launched together, with the same config file.
                  # The failure count is only expressed as a parameter in (every peer's) config file, and even the faulty peers will see
                  # Faulty=false in their config file. They need to derive their behavior from the Failures config field (and potentially
                  # the RandomSeed field).

# Low-level system parameters
loggingLevel="info"
peerTag="peers"
faultyPeerTag="faultyPeers"
minConcurrentRequests=$((256 * 16384)) # Based on empirical data. At saturation, makes the throughput-latency plot nicely go up (as it is equivalent to may concurrent clients).
requestBufferSizes="8192"
requestHandlerThreadNums="32"
messageBatchRates="4" # (in msgs/ms) The number of peers divided by this number gives the message batch timeout in ms
hardRateLimits="false"
earlyRequestVerification="false"
batchVerifiers="external" # possible values: sequential parallel external
throughputCap=131072000 # The system will always be proposing requests at a rate lower than this.
                      # Used to prevent view changes when too many batches accumulate in a bucket.

# System composition
orderers="Raft" # Possible values: Pbft HotStuff Raft Dummy
checkpointers="Signing"

# Parameters chosen for experiments
durations="240"             # [s]   !!! Don't forget to change the timeout in generate-master-commands.py if increasing this value !!!
bandwidths="1gbit"         # any value accepted by the tc command or "unlimited" !!! ATTENTION: Adapt MaxProposeDataRate in config accordingly !!!
payloadSizes="500"         # [Bytes]
fixedEpochLength=false
auths="false"
bucketsPerLeader="16"
minBuckets="16"
minEpochLength="256"       # [entries]
nodeConnections="1"
minConnections="16"
leaderPolicies="Simple"
leaderPolicyWithFaults="SimulatedRandomFailures"
crashTimings="EpochStart"

# For the single-leader policy, override the segment/epoch length
singleLeaderEpoch=$minEpochLength

# Parameters to tune:
batchsizes="4096"      # [requests]
batchrates="32"         # [batches/s]
minBatchTimeout="1000"     # [ms]
maxBatchTimeout="4000" # [ms]
segmentLengths="16"      # [entries]
viewChangeTimeouts="60000" # [ms]
nodeToLeaderRatios="1"

# batctimeout = minBatchTimeout, if maxLeaders/batchrate < minBatchTimeout
#               maxBatchTimeout, if maxLeaders/batchrate > maxBatchTimeout
#               numleaders/batchrate, otherwise
# maxLeaders = 1, if leaderPolicy = Single
#             = numPeers, otherwise

# Used to skip certain parameter combinations.
function skip() {
  return 1
}

throughputsAuthPbft=$()
throughputsAuthPbft[4]="8192"
throughputsAuthPbft[8]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthPbft[16]="8192"
throughputsAuthPbft[32]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthPbft[64]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthPbft[128]="1024 2046 8192 16384 32768 49152 57344 65536 73728"
throughputsNoAuthPbft=$()
throughputsNoAuthPbft[4]=""
throughputsNoAuthPbft[8]=""
throughputsNoAuthPbft[16]=""
throughputsNoAuthPbft[32]=""
throughputsNoAuthPbft[64]=""
throughputsNoAuthPbft[128]="1024 2046 8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthSinglePbft=$()
throughputsAuthSinglePbft[4]=""
throughputsAuthSinglePbft[8]=""
throughputsAuthSinglePbft[16]=""
throughputsAuthSinglePbft[32]=" 3072 4096 5120 6144 8192"
throughputsAuthSinglePbft[64]="     1024      2048 2560 3072 4096 5120"
throughputsAuthSinglePbft[128]="512 1024 1536 2048 2560 3072"
throughputsNoAuthSinglePbft=$()
throughputsNoAuthSinglePbft[4]=""
throughputsNoAuthSinglePbft[8]=""
throughputsNoAuthSinglePbft[16]=""
throughputsNoAuthSinglePbft[32]=""
throughputsNoAuthSinglePbft[64]=""
throughputsNoAuthSinglePbft[128]=""

throughputsAuthHotStuff=$()
throughputsAuthHotStuff[4]=""
throughputsAuthHotStuff[8]=""
throughputsAuthHotStuff[16]=""
throughputsAuthHotStuff[32]=""
throughputsAuthHotStuff[64]=""
throughputsAuthHotStuff[128]=""
throughputsNoAuthHotStuff=$()
throughputsNoAuthHotStuff[4]=""
throughputsNoAuthHotStuff[8]=""
throughputsNoAuthHotStuff[16]=""
throughputsNoAuthHotStuff[32]=""
throughputsNoAuthHotStuff[64]=""
throughputsNoAuthHotStuff[128]=""
throughputsAuthSingleHotStuff=$()
throughputsAuthSingleHotStuff[4]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthSingleHotStuff[8]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthSingleHotStuff[16]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthSingleHotStuff[32]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthSingleHotStuff[64]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsAuthSingleHotStuff[128]="8192 16384 32768 49152 57344 65536 73728 90112"
throughputsNoAuthSingleHotStuff=$()
throughputsNoAuthSingleHotStuff[4]=""
throughputsNoAuthSingleHotStuff[8]=""
throughputsNoAuthSingleHotStuff[16]=""
throughputsNoAuthSingleHotStuff[32]=""
throughputsNoAuthSingleHotStuff[64]=""
throughputsNoAuthSingleHotStuff[128]=""

throughputsAuthRaft=$()
throughputsAuthRaft[4]=""
throughputsAuthRaft[8]=""
throughputsAuthRaft[16]=""
throughputsAuthRaft[32]=""
throughputsAuthRaft[64]=""
throughputsAuthRaft[128]=""
throughputsNoAuthRaft=$()
throughputsNoAuthRaft[4]="8192 16384 32768 49152 57344 65536 73728"
throughputsNoAuthRaft[8]="8192 16384 32768 49152 57344 65536 73728"
throughputsNoAuthRaft[16]="8192 16384 32768 49152 57344 65536 73728"
throughputsNoAuthRaft[32]="8192 16384 32768 49152 57344 65536 73728"
throughputsNoAuthRaft[64]="88192 16384 32768 49152 57344 65536 73728"
throughputsNoAuthRaft[128]="8192 16384 32768 49152 57344 65536 73728"
throughputsAuthSingleRaft=$()
throughputsAuthSingleRaft[4]=""
throughputsAuthSingleRaft[8]=""
throughputsAuthSingleRaft[16]=""
throughputsAuthSingleRaft[32]=""
throughputsAuthSingleRaft[64]=""
throughputsAuthSingleRaft[128]=""
throughputsNoAuthSingleRaft=$()
throughputsNoAuthSingleRaft[4]=""
throughputsNoAuthSingleRaft[8]=""
throughputsNoAuthSingleRaft[16]=""
throughputsNoAuthSingleRaft[32]=""
throughputsNoAuthSingleRaft[64]="2500 2800"
throughputsNoAuthSingleRaft[128]="1536 2048"

# "Round" throughput values:
# 8192 16384 32768 49152
# 65536 81920 98304 114688
# 131072 147456 163840 180224
# 196608 212992 229376
#
# 16384 + 8192 = 24576
# 32768 + 8192 = 40960
# 49152 + 8192 = 57344
# 65566 + 8192 = 73728
# 81920 + 8192 = 90112

source scripts/global-vars.sh

batchTimeout() {
    if [ $leaderPolicy = "Single" ]; then
        value=$((1000 / batchrate)) # Don't multiply by numPeers, as only one peer proposes.
    else
        value=$((1000 * numPeers / batchrate)) # in milliseconds
    fi
    if [ $minBatchTimeout -ge $value ]; then
        echo $minBatchTimeout
    elif [ $maxBatchTimeout -le $value ]; then
        echo $maxBatchTimeout
    else
        echo $value
    fi
}

totalClients() {
    machines=$1
    instances=$2
    echo $(( $machines*$instances ))
}

perClientRate() {
    totalRate=$1
    totalClients=$2
    echo $(( $totalRate/$totalClients ))
}

requestsPerClient() {
    local duration=$1
    local totalRate=$2
    local totalClients=$3

    # The factor 2 is a margin to compensate for non-uniform client throughput.
    # Every client creates twice as many requests as would be needed to fill experiment duration
    # if the throughput was uniform across all clients.
    # The experiment duration itself is enforced separately by clients stopping after $duration seconds,
    # even if they did not submit all requests.
    echo $(( 2 * $duration*$totalRate/$totalClients ))
}

clientWatermarks() {
    totalClients=$1

    # Cummulatively, all clients have enough requests to fill 3/2 (one and a half) epochs
    # with requests before blocking on watermarks.
    wm=$(( 3 * $epoch * $batchsize / $totalClients / 2))

    # Enforce lower limit on concurrently submitted requests.
    if [ $wm -ge $((minConcurrentRequests / totalClients)) ]; then
      echo $wm
    else
      echo $((minConcurrentRequests / totalClients))
    fi
}

dplLines() {
    echo "# $exp,$numPeers,$numFailures,$viewChangeTimeout,$numLocations,$bandwidth,$numConnections,$orderer,$cpt,$machines,$instances,$totalclients,$segmentLength,$leaderPolicy,$epoch,$batchsize,$batchtimeout,$batchrate,$msgBatchPeriod,$numBuckets,$leaderBuckets,$auth,$verifyEarly,$throughputCap,$thr,$rate,$hardRateLimit,$requests,$watermark,$batchVerifier,$requestHandlers,$requestBufferSize" >> $deployment_file
    echo -n "run $exp config: config-$exp.yml   peers: $peerTag   clients: $clientTags" >> $deployment_file
    if [ $numFailures -gt 0 ] && ! $reuseFaulty; then
      echo -n " config: config-$exp-faulty.yml   peers: $faultyPeerTag" >> $deployment_file # Space at the start is crucial.
    fi
    echo -e "\n" >> $deployment_file
}

oldMirDplLines() {
    echo "run $exp oldmir   config: config-$exp-server.yml   peers: $peerTag   config: config-$exp-client.yml   clients: $clientTags" >> $deployment_file
    echo "" >> $deployment_file
}

config() {
  cat config-file-templates/mir-modular.yml | sed "s/LOGGINGLEVEL/$loggingLevel/ ; s/ORDERER/$orderer/ ; s/CHECKPOINTER/$cpt/ ; s/FAILURES/$numFailures/ ; s/PRIORITYCONNECTIONS/$numConnections/ ; s/VIEWCHANGETIMEOUT/$viewChangeTimeout/ ; s/CRASHTIMING/$crashTiming/ ; s/LEADERPOLICY/$leaderPolicy/ ; s/EPOCH/$epoch/ ; s/SEGMENTLENGTH/$segmentLength/ ; s/WATERMARK/$watermark/ ; s/BUCKETS/$numBuckets/ ; s/BATCHSIZE/$batchsize/ ; s/PAYLOAD/$payloadSize/ ; s/BATCHTIMEOUT/$batchtimeout/ ; s/THROUGHPUTCAP/$throughputCap/ ; s/MSGBATCHPERIOD/$msgBatchPeriod/ ; s/CLIENTS/$instances/ ; s/REQUESTS/$requests/ ; s/DURATION/$((duration * 1000))/ ; s/REQUESTRATE/$rate/ ; s/HARDRATELIMIT/$hardRateLimit/ ; s/BATCHVERIFIER/$batchVerifier/ ; s/REQUESTHANDLERTHREADS/$requestHandlers/ ; s/REQUESTINPUTBUFFER/$requestBufferSize/ ; s/AUTH/$auth/ ; s/VERIFYEARLY/$verifyEarly/ ; s/RANDOMSEED/$randomNumber/ ; s/FAULTY/false/ ; s/NLR/$nlr/" > $exp_data_dir/config/config-$exp.yml
  cat config-file-templates/mir-modular.yml | sed "s/LOGGINGLEVEL/$loggingLevel/ ; s/ORDERER/$orderer/ ; s/CHECKPOINTER/$cpt/ ; s/FAILURES/$numFailures/ ; s/PRIORITYCONNECTIONS/$numConnections/ ; s/VIEWCHANGETIMEOUT/$viewChangeTimeout/ ; s/CRASHTIMING/$crashTiming/ ; s/LEADERPOLICY/$leaderPolicy/ ; s/EPOCH/$epoch/ ; s/SEGMENTLENGTH/$segmentLength/ ; s/WATERMARK/$watermark/ ; s/BUCKETS/$numBuckets/ ; s/BATCHSIZE/$batchsize/ ; s/PAYLOAD/$payloadSize/ ; s/BATCHTIMEOUT/$batchtimeout/ ; s/THROUGHPUTCAP/$throughputCap/ ; s/MSGBATCHPERIOD/$msgBatchPeriod/ ; s/CLIENTS/$instances/ ; s/REQUESTS/$requests/ ; s/DURATION/$((duration * 1000))/ ; s/REQUESTRATE/$rate/ ; s/HARDRATELIMIT/$hardRateLimit/ ; s/BATCHVERIFIER/$batchVerifier/ ; s/REQUESTHANDLERTHREADS/$requestHandlers/ ; s/REQUESTINPUTBUFFER/$requestBufferSize/ ; s/AUTH/$auth/ ; s/VERIFYEARLY/$verifyEarly/ ; s/RANDOMSEED/$randomNumber/ ; s/FAULTY/true/ ; s/NLR/$nlr/" > $exp_data_dir/config/config-$exp-faulty.yml
}

oldMirConfig() {
    cp config-file-templates/oldmir-server.yml $exp_data_dir/config/config-$exp-server.yml
    cp config-file-templates/oldmir-client.yml $exp_data_dir/config/config-$exp-client.yml
}

csvLine() {
    echo "$exp,$numPeers,$nlr,$numFailures,$crashTiming,$viewChangeTimeout,$numLocations,$bandwidth,$numConnections,$orderer,$machines,$instances,$totalclients,$segmentLength,$leaderPolicy,$epoch,$batchsize,$batchtimeout,$batchrate,$msgBatchPeriod,$numBuckets,$leaderBuckets,$auth,$verifyEarly,$throughputCap,$thr,$rate,$hardRateLimit,$requests,$watermark,$batchVerifier,$requestHandlers,$requestBufferSize" >> $csv_file
}

generate() {
    exp=$(printf "%0${exp_id_digits}d" $exp_id)
    totalclients=$(totalClients $machines $instances)
    rate=$(perClientRate $thr $totalclients)
    requests=$(requestsPerClient $duration $thr $totalclients)
    watermark=$(clientWatermarks $totalclients)

    if [ $((totalclients * watermark * payloadSize)) -gt 8589934592 ]; then
      >&2 echo "Payload of potential in-flight requests exceeds 8GB!"
      >&2 echo "totalclients * watermark * payloadSize = $totalclients * $watermark * $payloadSize = $((totalclients * watermark * payloadSize))"
      exit 3
    fi

    if [ $leaderPolicy != "Single" ]; then
      local minBatchWaitingTime=$((1000 * batchsize * numPeers / throughputCap)) # in milliseconds
      if [ $minBatchWaitingTime -ge $batchtimeout ]; then
        >&2 echo "WARNING! Batch timeout ($batchtimeout) subsumed by throughput cap that imposes $minBatchWaitingTime."
      elif [ $minBatchWaitingTime -ge $((batchtimeout / 2)) ]; then
        >&2 echo "WARNING! Batch timeout ($batchtimeout) not more than double of what is imposed by throughput cap ($minBatchWaitingTime)."
      fi
      if [ $minBatchWaitingTime -ge $viewChangeTimeout ]; then
        >&2 echo "ERROR! View change timeout ($viewChangeTimeout) reaches the minimum batch waiting time ($minBatchWaitingTime)."
        exit 3
      elif [ $minBatchWaitingTime -ge $((viewChangeTimeout / 2)) ]; then
        >&2 echo "WARNING! View change timeout ($viewChangeTimeout) not more than double the minimum batch waiting time ($minBatchWaitingTime)."
      fi
    fi

    $(dplLines)
    $(config)
    $(csvLine)
}

function deployMachines() {
  local n=$1
  local tag=$2
  local alreadyStarted=$3
  local locations="$4"
  local i=0
  local offset=
  local machineTemplate
  local machineTemplates=""
  local perLocation=
  local toDeploy=
  local templateFile=
  numLocations=0 # This variable is intentionally global

  # Generate machine template files
  for location in $locations; do
    templateFile="$machineType-$location.cmt"
    cat $machineType | jq --arg dc "$location" '. + {datacenter: {name: $dc}}' > $templateFile
    machineTemplates="$machineTemplates $templateFile"
    numLocations=$((numLocations + 1))
  done
  perLocation=$((n / numLocations))

  # Calculate offset for adding "remaining" machines.
  offset=$((alreadyStarted % numLocations))

  for machineTemplate in $machineTemplates; do

    # If number of nodes is not divisible by number of locations, add an extra machine to some locations.
    if [ $(((i + numLocations - offset) % numLocations)) -lt $((n % numLocations)) ]; then
      toDeploy=$((perLocation + 1))
    else
      toDeploy=$perLocation
    fi
    i=$((i + 1))

    if [ $toDeploy -gt 0 ]; then
      echo "machine: $machineTemplate"
      echo "deploy $toDeploy $tag"
    fi
  done
}

function generateCombinations() {
  local numsOfInstances="$1"

  for bandwidth in $bandwidths; do
  echo "bandwidth: $bandwidth" >> $deployment_file
    for duration in $durations; do
      for leaderBuckets in $bucketsPerLeader; do
        numBuckets=$((leaderBuckets * numPeers))
        if [ $numBuckets -lt $minBuckets ]; then
          numBuckets=$minBuckets
        fi
        for instances in $numsOfInstances; do
          for leaderPolicy in $leaderPolicies; do
            targetLeaderPolicy=$leaderPolicy
            for segmentLength in $segmentLengths; do
              targetSegmentLength=$segmentLength
              for batchsize in $batchsizes; do
                for payloadSize in $payloadSizes; do
                  for nlr in $nodeToLeaderRatios; do
                    for batchrate in $batchrates; do
                       batchrate=$((batchrate * nlr))
                        batchtimeout=$(batchTimeout) # in milliseconds
                        for viewChangeTimeout in $viewChangeTimeouts; do
                          for crashTiming in $crashTimings; do
                            for numConnections in $nodeConnections; do
                              for msgBatchRate in $messageBatchRates; do
                                msgBatchPeriod=$((numPeers / msgBatchRate))
                                for hardRateLimit in $hardRateLimits; do
                                  for batchVerifier in $batchVerifiers; do
                                    for requestHandlers in $requestHandlerThreadNums; do
                                      for requestBufferSize in $requestBufferSizes; do
                                        for orderer in $orderers; do
                                          for cpt in $checkpointers; do
                                            for auth in $auths; do
                                              epoch=$((segmentLength * numPeers))

                                              if [ $epoch -lt $minEpochLength ]; then
                                                epoch=$minEpochLength
                                                segmentLength=$((epoch / numPeers))
                                              fi

                                              if $fixedEpochLength; then
                                                segmentLength=0
                                              fi

                                              if [ $numFailures -gt 0 ]; then
                                                leaderPolicy="$leaderPolicyWithFaults"
                                              fi

                                              if [ $leaderPolicy = "Single" ]; then
                                                segmentLength=$singleLeaderEpoch
                                                epoch=$singleLeaderEpoch
                                                msgBatchPeriod=$((4 / msgBatchRate)) # Msg batch rate as if there were only 4 peers.
                                                numBuckets=$bucketsPerLeader
                                              fi

                                              # Make sure the minimal number of connections of a peer is reached.
                                              while [ $((numConnections * numPeers)) -lt $minConnections ]; do
                                                numConnections=$((numConnections + 1))
                                              done

                                              if [ "$auth" = "true" ]; then
                                                for verifyEarly in $earlyRequestVerification; do
                                                  if [ "$orderer" = "Pbft" ]; then
                                                    if [ $leaderPolicy = "Single" ]; then
                                                      throughputs=${throughputsAuthSinglePbft[$numPeers]}
                                                    else
                                                      throughputs=${throughputsAuthPbft[$numPeers]}
                                                    fi
                                                  elif [ $orderer = "HotStuff" ]; then
                                                    if [ $leaderPolicy = "Single" ]; then
                                                      throughputs=${throughputsAuthSingleHotStuff[$numPeers]}
                                                    else
                                                      throughputs=${throughputsAuthHotStuff[$numPeers]}
                                                    fi
                                                  elif [ $orderer = "Raft" ]; then
                                                    if [ $leaderPolicy = "Single" ]; then
                                                      throughputs=${throughputsAuthSingleRaft[$numPeers]}
                                                    else
                                                      throughputs=${throughputsAuthRaft[$numPeers]}
                                                    fi
                                                  fi
                                                  for thr in $throughputs; do
                                                    if ! skip; then
                                                      $(generate)
                                                      (( exp_id += 1 ))
                                                    fi
                                                  done
                                                done
                                              elif [ "$auth" = "false" ]; then
                                                verifyEarly="false"
                                                if [ "$orderer" = "Pbft" ]; then
                                                  if [ $leaderPolicy = "Single" ]; then
                                                    throughputs=${throughputsNoAuthSinglePbft[$numPeers]}
                                                  else
                                                    throughputs=${throughputsNoAuthPbft[$numPeers]}
                                                  fi
                                                elif [ $orderer = "HotStuff" ]; then
                                                  if [ $leaderPolicy = "Single" ]; then
                                                    throughputs=${throughputsNoAuthSingleHotStuff[$numPeers]}
                                                  else
                                                    throughputs=${throughputsNoAuthHotStuff[$numPeers]}
                                                  fi
                                                elif [ $orderer = "Raft" ]; then
                                                  if [ $leaderPolicy = "Single" ]; then
                                                    throughputs=${throughputsNoAuthSingleRaft[$numPeers]}
                                                  else
                                                    throughputs=${throughputsNoAuthRaft[$numPeers]}
                                                  fi
                                                fi
                                                for thr in $throughputs; do
                                                  if ! skip; then
                                                    $(generate)
                                                    (( exp_id += 1 ))
                                                  fi
                                                done
                                              else
                                                >&2 echo "Valid values for auth are 'true' and 'false'. Received: $auth"
                                                exit 2
                                              fi
                                          done
                                        done
                                      done
                                    done
                                  done
                                done
                              done
                            done
                          done
                        done
                      done
                    done
                  done
                done
              done
            done
          done
        done
      done
    done
  done
}

# Obtain target directory and output file names
if [ -n "$1" ]; then
  exp_data_dir=$1
  shift
else
  exp_data_dir="generated-experiment-config"
  mkdir -p $exp_data_dir
fi
deployment_file="$exp_data_dir/$dpl_filename"
csv_file="$exp_data_dir/$csv_filename"


# Obtain the ID of the first experiment to generate, if any was given.
# Otherwise, start at zero.
if [ -n "$1" ]; then
  exp_id_offset=$1
  exp_id=$1
  shift
else
  exp_id_offset=0
  exp_id=0
fi


# Check if some configuration already exists in the target directory.
# Fail if it does, to avoid accidental overwriting.
if [ -d "$exp_data_dir/config" ]; then
  >&2 echo "generate-config.sh: config file directory '$exp_data_dir/config' already exists, risk of overwriting"
  exit 1
else
  mkdir "$exp_data_dir/config"
fi


# Generate header of the deployment file.
{
  echo "# Generated deployment file"
  echo "# "
  echo "# systemSizes:      $systemSizes"
  echo "# machineType:      $machineType"
  echo "# bandwidths:       $bandwidths"
  echo "# "
  echo "# segmentLengths:   $segmentLengths"
  echo "# bucketsPerLeader: $bucketsPerLeader"
  echo "# batchsizes:       $batchsizes"
  echo "# auths:            $auths"
  echo "# clients8:         $clients8"
  echo "# clients16:        $clients16"
  echo "# duration:         $duration"
  echo "# peerTag:          $peerTag"
  echo ""

  clientsStarted=0
  if [ -n "$clients1" ]; then
    deployMachines 1 1client $clientsStarted "$machineLocations"
    clientsStarted=$((clientsStarted + 1))
  fi

  if [ -n "$clients16" ] || [ -n "$clients32" ]; then
    deployMachines 16 16clients $clientsStarted "$machineLocations"
    clientsStarted=$((clientsStarted + 16))
  fi

  if [ -n "$clients32" ]; then
    deployMachines 16 16extraclients $clientsStarted "$machineLocations"
    clientsStarted=$((clientsStarted + 16))
  fi

  echo ""
} > $deployment_file


# Generate CSV file header.
echo "exp,peers,nlr,failures,crash-timing,vctimeout,datacenters,bandwidth,num-connections,orderer,clients,instances,client-threads,segment,leader-policy,epoch,batchsize,batchtimeout,batchrate,msgbatchperiod,buckets,leader-buckets,authentication,verify-early,throughput-cap,target-throughput,rate-per-client,hard-rate-limit,requests,cl-watermarks,batch-verifier,request-handlers,req-buffer-size" > $csv_file
echo -e "\n# Parameters: exp,peers,nlr,failures,crash-timing,vctimeout,datacenters,bandwidth,num-connections,orderer,clients,instances,client-threads,leader-policy,segment,epoch,batchsize,batchtimeout,batchrate,msgbatchperiod,buckets,leader-buckets,authentication,verify-early,throughput-cap,target-throughput,rate-per-client,hard-rate-limit,requests,cl-watermarks,batch-verifier,request-handlers,req-buffer-size\n" >> $deployment_file

deployedCorrect=0
deployedFaulty=0
for numPeers in $systemSizes; do

  randomNumber=$RANDOM # Keep the same random seed for a whole set of experiments to be able to compare the runs.

  numFailures=${failureCounts[0]}
  failureCounts=(${failureCounts[@]:1})

  if $reuseFaulty; then
    numPeers=$((numPeers + numFailures))
  fi

  {
  echo ""
  echo "# ========================================"
  echo "#  Peers: $numPeers"
  echo "# Faulty: $numFailures"
  echo "#  Reuse: $reuseFaulty"
  echo "# ========================================"

  deployMachines $((numPeers - deployedCorrect)) $peerTag $deployedCorrect "$machineLocations"
  deployedCorrect=$numPeers

  if [ $numFailures -gt $deployedFaulty ] && ! $reuseFaulty; then
    deployMachines $((numFailures - deployedFaulty)) $faultyPeerTag $deployedFaulty "$faultyMachineLocations"
    deployedFaulty=$numFailures
  fi

  echo ""
  } >> $deployment_file

  machines=1
  clientTags="1client"
  generateCombinations "$clients1"

  machines=16
  clientTags="16clients"
  generateCombinations "$clients16"

  machines=32
  clientTags="16clients 16extraclients"
  generateCombinations "$clients32"

done

echo "Generated $((exp_id - exp_id_offset)) experiments."

