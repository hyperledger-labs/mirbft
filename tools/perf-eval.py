"""
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import subprocess, os, datetime, sys, collections
import numpy as np

class Block:
    def __init__(self):
        self.id = None
        self.leader = False
        self.preprepare = None
        self.preprepared = None
        self.prepared = None
        self.committed = None
        self.delivered = None
        self.transactions = []
        self.preprepareAndPrepare = None
        self.commit = None
        self.checkpoint = None
        self.size = None
    def __str__(self):
        return "Block id:"+str(self.id)

class Transaction:
    def __init__(self):
        self.id = None
        self.src = None
        self.batchId = None
        self.sent = None
        self.received = None
        self.batch = None
        self.client2server  = None
        self.pending = None
        self.latency = None
        self.delivered = None

    def __str__(self):
        return "Transaction <id:"+str(self.id)+", sent:"+str(self.sent)+", delivered:"+str(self.delivered)+", from:"+str(self.src)+">"
def main(argv):
    servers = int(argv[0])
    clients = int(argv[1])
    serverLogs = []
    clientLogs = []
    for i in range(servers):
        serverLogs.append(argv[2+i])
    for i in range(clients):
        clientLogs.append(argv[2+servers+i])
    offset = 0 # from where to start
    tail = 1 # how many to omit
    if len(argv) > servers+clients+2 :
        offset = int(argv[servers+clients+2])
    if len(argv) > servers+clients+3 :
        tail = int(argv[servers+clients+3])
    transactions = {}
    blocks = {}
    for sl in serverLogs:
        with open(sl) as fs:
            serverContent = fs.readlines()
            serverContent = [x.strip() for x in serverContent]
            for line in serverContent:
                if 'DELIVERED' in line:
                    tx = Transaction()
                    tx.id = line.split()[-3]
                    transactions[tx.id] = tx
                    tx.src = line.split()[-1]
            for line in serverContent:
                if 'DELIVERED' in line:
                    year, month, day = line.split()[0].split('/')
                    hour, minute, second = line.split()[1].split(':')
                    micro = int(second.split('.')[1])
                    second = second.split('.')[0]
                    batchId = int(line.split()[-1])
                    if batchId not in blocks:
                        block = Block()
                        block.id = batchId
                        blocks[batchId] = block
                    block = blocks[batchId]
                    txId = line.split()[-3]
                    if txId in transactions:
                        if transactions[txId].delivered is None:
                            transactions[txId].delivered = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micro)
                        block.transactions.append(tx)
                if 'DELIVERING' in line:
                    batchId = int(line.split()[-4])
                    if batchId not in blocks:
                        block = Block()
                        block.id = batchId
                        blocks[batchId] = block
                    block = blocks[batchId]
                    year, month, day = line.split()[0].split('/')
                    hour, minute, second = line.split()[1].split(':')
                    micro = int(second.split('.')[1])
                    second = second.split('.')[0]
                    block.delivered = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micro)
                    block.size = int(line.split()[-2])
    rate = []
    for cl in clientLogs:
        with open(cl) as fc:
            clientContent = fc.readlines()
            clientContent = [x.strip() for x in clientContent]
            reqs = end = start = 0
            for line in clientContent:
                if 'SENDING' in line:
                    txId = line.split()[-3]
                    src = line.split()[-1]
                    if txId in transactions:
                        tx = transactions[txId]
                        year, month, day = line.split()[0].split('/')
                        hour, minute, second = line.split()[1].split(':')
                        micro = int(second.split('.')[1])
                        second = second.split('.')[0]
                        tx.sent = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micro)
                if 'START' in line:
                    year, month, day = line.split()[0].split('/')
                    hour, minute, second = line.split()[1].split(':')
                    micro = int(second.split('.')[1])
                    second = second.split('.')[0]
                    start = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micro)
                if  'FINISH' in line:
                    reqs = float(line.split()[-1])
                    year, month, day = line.split()[0].split('/')
                    hour, minute, second = line.split()[1].split(':')
                    micro = int(second.split('.')[1])
                    second = second.split('.')[0]
                    end = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micro)
            duration = (end-start).total_seconds()
            if duration != 0 :
                rate.append(reqs/(end-start).total_seconds())

    latency = []
    delivery = []
    block_delivery = {}
    throughput = []

    ord_blocks = collections.OrderedDict(sorted(blocks.items()))

    for key, block in ord_blocks.iteritems():
        if block.delivered is not None:
            block_delivery[key] = block.delivered
            if key > 1:
                tpt = float(block.size)/(block_delivery[key]-block_delivery[key-1]).total_seconds()
                throughput.append(tpt)
            for i in range(block.size):
                delivery.append(block.delivered)
    for tx in transactions.values():
        if tx.sent is not None and tx.delivered is not None:
            tx.latency = (tx.delivered - tx.sent).total_seconds()*1000
            latency.append(tx.latency)
    latency.sort()
    f = open("latency.out", "w")
    for i in range(len(latency)):
        f.write(str(latency[i])+" "+str(float(i)/(len(latency)))+"\n")
    latency_avg = np.mean(latency)
    print "End to end latency: " + str(latency_avg) + " ms"
    rate_avg = np.mean(rate)
    print "Average request rate per client: " + str(rate_avg) + " r/s"

    delivery.sort()
    delivery = delivery[offset:0-tail]
    thr = float(len(delivery))/(delivery[-1]-delivery[0]).total_seconds()
    print "Experiment duration: "+str((delivery[-1]-delivery[0]).total_seconds())+" s"
    print "Throughput: " + str(thr)  + " r/s"
    print "Requests: " + str(len(delivery))
    # inst_thr = np.mean(throughput)
    # print "Avg instant throughput: " + str(inst_thr)  + " tps"

if __name__ == "__main__":
    main(sys.argv[1:])