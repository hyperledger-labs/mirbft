import sys
import math
import csv
from collections import defaultdict
import matplotlib

# Disable interactive mode, as this script is used for generating image files.
matplotlib.interactive(False)
matplotlib.use("agg") # "agg" or "pdf" for PNG or PDF output, respectively.

import matplotlib.pyplot as plt

def plotFile(fileName, axes, bucketSize, y_sampling, start=None, end=None):
    hist = defaultdict(lambda : 0)
    maxVal = -math.inf

    print("Reading CSV file: {0}".format(fileName))
    with open(fileName) as csvFile:
        vals = []
        cnts = []
        maxVal = -math.inf

        for row in csv.reader(csvFile):
            val = int(row[0])
            cnt = float(row[1])

            vals.append(val)
            cnts.append(cnt)
            if val > maxVal:
                maxVal = val

        tpt = defaultdict(lambda : 0)
        for i in range(len(vals)):
            tpts = []
            first = vals[i]
            next_vals = vals[i:]
            for j in range(len(next_vals)):
                if next_vals[j] - first >= bucketSize:
                    break
                tpts.append(cnts[i+j])
            tpt[vals[i]] = float(sum(tpts)) / bucketSize
    y = [tpt[i] * y_sampling for i in tpt]
    x = [float(i) / 1000 for i in vals]


    axes.plot(x, y, linewidth=1, color='k')

# Name of the file where the resulting image will be written
outFileName = sys.argv[1]

# Size of the histogram bucket.
# All values are truncated to the next lowest multiple of the bucket size
# (By performing an integer division (//) by the bucketSize)
bucketSize = int(sys.argv[2])
y_sampling = int(sys.argv[3])
if sys.argv[4] == '-':
    start = None
else:
    start = float(sys.argv[4])
if sys.argv[5] == '-':
    end = None
else:
    end = float(sys.argv[5])

fig, ax = plt.subplots()

ax.spines['bottom'].set_position('zero')
ax.spines['left'].set_position('zero')

if start is not None:
    ax.spines['left'].set_position(('data', start))
    ax.set_xlim(left=start)
if end is not None:
    ax.set_xlim(right=end)

ax.set_xlabel("time (s)".format(bucketSize))
ax.set_ylabel("throughput (kreq/s)")

#ax.set_ylabel("throughput (reqs/s) averaged over "+ str(float(bucketSize)/1000) +" s")

ax.grid(which='both')

for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
             ax.get_xticklabels() + ax.get_yticklabels()):
    item.set_fontsize(16)


for fileName in sys.argv[6:]:
    plotFile(fileName, ax, bucketSize, y_sampling, start, end)

fig.set_size_inches(8, 4.5)
fig.savefig(outFileName, dpi=400)
