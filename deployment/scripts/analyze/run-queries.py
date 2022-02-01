import sqlite3
import math
import sys
import time

class StdevFunc:
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 1

    def step(self, value):
        if value is None:
            return
        tM = self.M
        self.M += (value - tM) / self.k
        self.S += (value - tM) * (value - self.M)
        self.k += 1

    def finalize(self):
        if self.k < 3:
            return None
        return math.sqrt(self.S / (self.k-2))


def readQueries(fileName):
    queries = []
    query = ""
    export = None

    with open(fileName) as f:
        for l in f:
            if l.startswith("-- export"):
                export = l[len("-- export"):].strip()

            if l.strip() != "":
                query += l
            else:
                if query != "":
                    yield query.rstrip(), export
                    query = ""
                    export = None
    if query != "":
        yield query.rstrip(), export

dbFile = sys.argv[1]
queryFile = sys.argv[2]
exportDir = None
if len(sys.argv) > 3:
    exportDir = sys.argv[3]
totalTime=0

with sqlite3.connect(dbFile) as con:

    con.create_aggregate("stdev", 1, StdevFunc)
    cur = con.cursor()

    # This is necessary for rollback even if autocommit mode is disabled (it should be by default).
    # If the user queries create views / tables, these would be persisted without this.
    cur.execute("BEGIN;")


    for q, export in readQueries(sys.argv[2]):

        print(q)
        start_time = time.process_time()
        rows = cur.execute(q).fetchall()
        end_time = time.process_time()
        print("Query time: {:.3f} seconds".format(end_time - start_time))
        totalTime += end_time - start_time


        if exportDir is not None and export is not None:
            with open("{0}/{1}".format(exportDir, export), "w") as e:
                for row in rows:
                    print(", ".join(str(val) for val in row))
                    e.write(", ".join(str(val) for val in row) + "\n")
        else:
            for row in rows:
                print(", ".join(str(val) for val in row))

        print("Rows returned: {0}".format(len(rows)))
        print()
    print("Total running time: {:.3f} seconds".format(totalTime))

    # Revert the the database to the initial state.
    con.rollback()
