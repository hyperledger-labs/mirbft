import sqlite3
import sys
import json

def loadDataFile(fileName, db):
    #print("   Reading file: {0}".format(fileName))

    # Read JSON lines from file
    with open(fileName) as f:
        events_json = [json.loads(l) for l in f]

    #print("Processing file: {0}".format(fileName))

    protocol_event_types = {"PROPOSE", "PREPREPARE", "COMMIT", "MSG_BATCH", "BANDWIDTH", "BUCKET_STATE", "NEW_EPOCH", "VIEW_CHANGE"}
    request_event_types = {"CLIENT_SLACK", "REQ_SEND", "REQ_RECEIVE", "RESP_SEND", "RESP_RECEIVE", "ENOUGH_RESP", "REQ_FINISHED", "REQ_DELIVERED"}
    ethereum_event_types = {"ETH_VOTE_SUBMIT", "ETH_VOTE_DONE"}
    cpuusage_event_types = {"CPU_USAGE"}

    num_events = 0

    # Insert request events into DB
    events_tuples = [(j["time"], j["message"], j["nodeId"], j["sampledVal"], j["val0"]) for j in events_json if j["message"] in request_event_types]
    db.executemany("INSERT INTO request(ts, event, nodeID, clSn, latency) values(?,?,?,?,?)", events_tuples)
    num_events += len(events_tuples)
    #print(" Request events: {0}".format(num_request_events))

    # Insert protocol events into DB
    events_tuples = [(j["time"], j["message"], j["nodeId"], j["sampledVal"], j["val0"]) for j in events_json if j["message"] in protocol_event_types]
    db.executemany("INSERT INTO protocol(ts, event, nodeID, seqNr, val) values(?,?,?,?,?)", events_tuples)
    num_events += len(events_tuples)
    #print("Protocol events: {0}".format(num_protocol_events))

    # Insert ethereum events into DB
    events_tuples = [(j["time"], j["message"], j["nodeId"], j["sampledVal"], j["val0"]) for j in events_json if j["message"] in ethereum_event_types]
    db.executemany("INSERT INTO ethereum(ts, event, nodeID, configNr, gasCost) values(?,?,?,?,?)", events_tuples)
    num_events += len(events_tuples)
    #print(" Request events: {0}".format(num_request_events))

    # Insert CPU usage events into DB
    events_tuples = [(j["time"], j["message"], j["nodeId"], j["sampledVal"], j["val0"]) for j in events_json if j["message"] in cpuusage_event_types]
    db.executemany("INSERT INTO cpuusage(ts, event, nodeID, total, system) values(?,?,?,?,?)", events_tuples)
    num_events += len(events_tuples)
    #print(" Request events: {0}".format(num_request_events))

    return num_events


def createTables(db):
    cur.execute('DROP TABLE IF EXISTS protocol')

    cur.execute('DROP TABLE IF EXISTS request')

    cur.execute('DROP TABLE IF EXISTS ethereum')

    cur.execute('DROP TABLE IF EXISTS cpuusage')

    cur.execute('CREATE TABLE protocol (\
    "ts" INTEGER,\
    "event" TEXT,\
    "nodeId" INTEGER,\
    "seqNr" INTEGER,\
    "val" INTEGER\
    )')

    cur.execute('CREATE TABLE request (\
    "ts" INTEGER,\
    "event" TEXT,\
    "nodeId" INTEGER,\
    "clSn" INTEGER,\
    "latency" INTEGER\
    )')

    cur.execute('CREATE TABLE ethereum (\
    "ts" INTEGER,\
    "event" TEXT,\
    "nodeId" INTEGER,\
    "configNr" INTEGER,\
    "gasCost" INTEGER\
    )')

    cur.execute('CREATE TABLE cpuusage (\
    "ts" INTEGER,\
    "event" TEXT,\
    "nodeId" INTEGER,\
    "total" INTEGER,\
    "system" INTEGER\
    )')


dbFile = sys.argv[1]

with sqlite3.connect(dbFile) as con:

    cur = con.cursor()
    createTables(cur)

    num_events=0
    for inFileName in sys.argv[2:]:
        num_events += loadDataFile(inFileName, cur)
    print("    Events loaded: {0}".format(num_events))

    con.commit()
