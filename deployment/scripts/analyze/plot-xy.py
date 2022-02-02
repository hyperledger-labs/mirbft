import sys
import csv

import matplotlib
# Disable interactive mode, as this script is used for generating image files.
matplotlib.interactive(False)
matplotlib.use("agg") # "agg" or "pdf" for PNG or PDF output, respectively.
import matplotlib.pyplot as plt

outFileName = "plot.png"
valFields = {'exp', 'batchtimeout', 'msgbatchperiod', 'cl-watermarks', 'buckets', 'num-connections', 'requests', 'rate-per-client', 'batch-size-10pctile-trunc', 'batch-size-90pctile-trunc', 'batch-size-avg-trunc', 'client-slack-1pctile-raw', 'client-slack-1pctile-trunc', 'client-slack-avg-raw', 'client-slack-avg-trunc', 'client-slack-stdev-raw', 'client-slack-stdev-trunc', 'commit-rate-raw', 'commit-rate-trunc', 'cpu-system', 'cpu-total', 'duration-raw', 'duration-trunc', 'epochs-avg', 'epochs-max', 'epochs-min', 'latency-95pctile-raw', 'latency-95pctile-raw-nowm', 'latency-95pctile-trunc', 'latency-95pctile-trunc-nowm', 'latency-avg-raw', 'latency-avg-raw-nowm', 'latency-avg-trunc', 'latency-avg-trunc-nowm', 'latency-stdev-raw', 'latency-stdev-raw-nowm', 'latency-stdev-trunc', 'latency-stdev-trunc-nowm', 'msg-batch-avg-trunc', 'nreq-raw', 'nreq-trunc', 'propose-rate-raw', 'propose-rate-trunc', 'throughput-raw', 'throughput-trunc', 'viewchanges-avg', 'viewchanges-total', 'latency-avg-shortened-raw', 'latency-95pctile-shortened-raw'}

# DEBUG: temporarily don't consider some fields as a parameters
# valFields.add("epoch")
# valFields.add("clients")


def filterData(data, f):
    return [d for d in data if all(d[key] == val for key, val in f.items())]

def conflictParams(line):
    if len(line) < 2:
        return []
    else:
        keys = set()
        for i in range(0, len(line)-1):
            keys |= {key for key in paramNames if line[i][key] != line[i+1][key]}
        return keys - {sortField}

def splitLine(line):
    params = conflictParams(line)
    filters = [{}]
    while params:
        key = params.pop()
        newFilters = []
        for f in filters:
            newFilters += [{**f, key: val} for val in {dp[key] for dp in line}]
        filters = newFilters

    return [(f, sorted(filterData(line, f), key=lambda p: int(p[sortField]))) for f in filters]


def printLines(data):
    for f, line in splitLine(data):
        print()
        print(f)
        for dp in line:
            print(dp["exp"], dp[sortField], dp[xField], dp[yField])


def plotLine(ax, line, label):
    # Ignore empty lines
    if len(line) == 0:
        return

    # print([dp["exp"] for dp in line])
    # print([dp[xField] for dp in line])
    # print([dp[yField] for dp in line])
    d = [
        {"x": float(dp[xField]),
         "y": float(dp[yField]),
         "exp": dp["exp"]}
        for dp in line if dp[xField] and dp[yField]
    ]

    ax.plot([val["x"] for val in d], [val["y"] for val in d], '-o', label=str(label), linewidth=0.5)
    for val in d:
        ax.annotate(val["exp"], (val["x"], val["y"]))
    ax.legend(loc='upper left', fontsize="x-small")


csvFile = sys.argv[1]
sortField = sys.argv[2]
xField = sys.argv[3]
yField = sys.argv[4]


# Load data from CSV file
with open(csvFile) as f:
    resultReader = csv.DictReader(f)
    data = [dict(row) for row in resultReader]

# Load ordered list of field names
with open(csvFile) as f:
    for line in f:
        paramNames = [field.strip() for field in line.strip().split(",") if field.strip() not in valFields]
        break

print("Loaded data points: ", len(data))

# Filter data using constraints passed on the command line and sort the filtered data by target throughput
constraints = {arg.split("=")[0]: arg.split("=")[1] for arg in sys.argv[5:]}
data = filterData(data, constraints)
data.sort(key=lambda d: int(d[sortField]))

print("Filtered data points: ", len(data))

printLines(data)

fig, ax = plt.subplots()

# ax.spines['bottom'].set_position('zero')
# ax.spines['left'].set_position('zero')
ax.grid(which='both')

for f, line in splitLine(data):
    plotLine(ax, line, f)

fig.savefig(outFileName, dpi=400)
