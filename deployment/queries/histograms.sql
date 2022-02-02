-- Truncate request table to include only rows with timestamps between:
--   the first response obtained by the last client to obtain a response
--   and
--   the last request obtained by the client first to finish sending requests
-- Then, cut off 5 second from each end.
-- We could use a view here too, but a physical table is much faster to access.
-- The script processing this file normally makes sure that
-- changes made by this script to the database are rolled back and are not persisted.
CREATE TABLE request_truncated as
SELECT *
FROM request
WHERE
  ts - 5000000 >= (SELECT max(t)
    FROM (SELECT min(ts) as t
      FROM request
      WHERE event = 'REQ_FINISHED'
      GROUP BY nodeId))
  AND ts + 5000000 <= (SELECT min(t)
    FROM (SELECT max(ts) as t
      FROM request
      WHERE event = 'REQ_SEND'
      GROUP BY nodeId))

-- Do the same as above with the protocol table.
-- Note that the truncation times are still taken from the request table.
CREATE TABLE protocol_truncated as
SELECT *
FROM protocol
WHERE
  ts - 5000000 >= (SELECT max(t)
    FROM (SELECT min(ts) as t
      FROM request
      WHERE event = 'REQ_FINISHED'
      GROUP BY nodeId))
  AND ts + 5000000 <= (SELECT min(t)
    FROM (SELECT max(ts) as t
      FROM request
      WHERE event = 'REQ_SEND'
      GROUP BY nodeId))

-- Do the same as above with the CPU usage table.
-- Note that the truncation times are still taken from the request table.
CREATE TABLE cpuusage_truncated as
SELECT *
FROM cpuusage
WHERE
  ts - 5000000 >= (SELECT max(t)
    FROM (SELECT min(ts) as t
      FROM request
      WHERE event = 'REQ_FINISHED'
      GROUP BY nodeId))
  AND ts + 5000000 <= (SELECT min(t)
    FROM (SELECT max(ts) as t
      FROM request
      WHERE event = 'REQ_SEND'
      GROUP BY nodeId))


-- Total CPU usage in time (peer 0)
-- export timeline-cpu-usage-total-peer-0.csv
SELECT (ts - (SELECT min(ts) FROM cpuusage WHERE event = 'CPU_USAGE' AND nodeId = 0))/1000 as msec, total
from cpuusage
WHERE event = 'CPU_USAGE' AND nodeId = 0
-- (msec, totalcpu[%])

-- Total CPU usage in time (peer 1)
-- export timeline-cpu-usage-total-peer-1.csv
SELECT (ts - (SELECT min(ts) FROM cpuusage WHERE event = 'CPU_USAGE' AND nodeId = 1))/1000 as msec, total
from cpuusage
WHERE event = 'CPU_USAGE' AND nodeId = 1
-- (msec, totalcpu[%])

-- Total CPU usage in time (average over all peers)
-- export timeline-cpu-usage-total.csv
SELECT (ts - (SELECT min(ts) FROM cpuusage WHERE event = 'CPU_USAGE'))/1000 as msec, avg(total)
from cpuusage
WHERE event = 'CPU_USAGE'
GROUP BY msec
-- (msec, totalcpu[%])

-- System CPU usage in time (peer 0). This is the CPU usage reported under "System" in /proc/stat
-- export timeline-cpu-usage-system-peer-0.csv
SELECT (ts - (SELECT min(ts) FROM cpuusage WHERE event = 'CPU_USAGE' AND nodeId = 0))/1000 as msec, system
from cpuusage
WHERE event = 'CPU_USAGE' AND nodeId = 0
-- (msec, systemcpu[%])

-- System CPU usage in time (peer 1). This is the CPU usage reported under "System" in /proc/stat
-- export timeline-cpu-usage-system-peer-1.csv
SELECT (ts - (SELECT min(ts) FROM cpuusage WHERE event = 'CPU_USAGE' AND nodeId = 1))/1000 as msec, system
from cpuusage
WHERE event = 'CPU_USAGE' AND nodeId = 1
-- (msec, systemcpu[%])

-- System CPU usage in time (average over all peers). This is the CPU usage reported under "System" in /proc/stat
-- export timeline-cpu-usage-system.csv
SELECT (ts - (SELECT min(ts) FROM cpuusage WHERE event = 'CPU_USAGE'))/1000 as msec, avg(system)
from cpuusage
WHERE event = 'CPU_USAGE'
GROUP BY msec
-- (msec, systemcpu[%])

-- Batches committed in time (peer 0)
-- export timeline-commit-peer-0.csv
--
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'COMMIT' AND nodeId = 0))/1000 as msec, count()
FROM protocol
WHERE event = 'COMMIT' AND nodeId = 0
GROUP BY msec
-- (msec, count[req])

-- Batches committed in time (all peers)
-- export timeline-commit-all.csv
--
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'COMMIT'))/1000 as msec, count()
FROM protocol
WHERE event = 'COMMIT'
GROUP BY msec
-- (msec, count[req])

-- Client 0 request sending in time
-- export timeline-submit-client-1.csv
--
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'REQ_SEND' AND nodeId = -1))/1000 as msec, count()
FROM request
WHERE event = 'REQ_SEND' AND nodeId = -1
GROUP BY msec
-- (msec, count[req])

-- Client 1 request sending in time
-- export timeline-submit-client-2.csv
--
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'REQ_SEND' AND nodeId = -2))/1000 as msec, count()
FROM request
WHERE event = 'REQ_SEND' AND nodeId = -2
GROUP BY msec
-- (msec, count[req])

-- Client request sending in time (all clients)
-- export timeline-submit-all.csv
--
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'REQ_SEND'))/1000 as msec, count()
FROM request
WHERE event = 'REQ_SEND'
GROUP BY msec
-- (msec, count[req])

-- Latency histogram (by millisecond buckets) for client 0
-- export histogram-latency-client-1.csv
--
SELECT latency/1000 as lat, count()
FROM request
WHERE event = 'REQ_FINISHED' AND nodeId = -1
GROUP BY lat
--(latency[ms], count[req])

-- Latency histogram (by millisecond buckets), all clients combined
-- export histogram-latency-all.csv
--
SELECT latency/1000 as lat, count()
FROM request
WHERE event = 'REQ_FINISHED'
GROUP BY lat
--(latency[ms], count[req])

-- Latency histogram (by millisecond buckets) for client 0, not considering watermark blocking
-- export histogram-latency-client-1.csv
--
SELECT latency/1000 as lat, count()
FROM request
WHERE event = 'ENOUGH_RESP' AND nodeId = -1
GROUP BY lat
--(latency[ms], count[req])

-- Latency histogram (by millisecond buckets), all clients combined, not considering watermark blocking
-- export histogram-latency-all.csv
--
SELECT latency/1000 as lat, count()
FROM request
WHERE event = 'ENOUGH_RESP'
GROUP BY lat
--(latency[ms], count[req])

-- Throughput in time (per millisecond)
-- export timeline-throughput-commit.csv
--
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'REQ_FINISHED'))/1000 as msec, count()
FROM request
WHERE event = 'REQ_FINISHED'
GROUP BY msec
-- (msec, count[req])

-- Throughput in time (per millisecond), in order
-- export timeline-throughput-deliver.csv
--
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'REQ_DELIVERED'))/1000 as msec, count()
FROM request
WHERE event = 'REQ_DELIVERED'
GROUP BY msec
-- (msec, count[req])

-- Batch size histogram
-- export batch-sizes.csv
--
SELECT val, count()
FROM protocol
WHERE event = 'PROPOSE'
GROUP BY val
-- (batch-size[req], num-batches)

-- Batch size in time (per millisecond), all peers
-- export timeline-batch-size.csv
--
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'PROPOSE'))/1000 as msec, avg(val)
FROM protocol
WHERE event = 'PROPOSE'
GROUP BY msec
-- (msec, avg[req])

-- Proposals in time (per millisecond), all peers
-- export timeline-propose.csv
--
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'PROPOSE'))/1000 as msec, count()
FROM protocol
WHERE event = 'PROPOSE'
GROUP BY msec
-- (msec, avg[req])

-- Proposals in time (per millisecond), all peers
-- export timeline-propose-peer-0.csv
--
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'PROPOSE'))/1000 as msec, count()
FROM protocol
WHERE event = 'PROPOSE' AND nodeId = 0
GROUP BY msec
-- (msec, avg[req])

-- Batch size in time (per millisecond), peer 0
-- export timeline-batch-size-peer-0.csv
--
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'PROPOSE'))/1000 as msec, avg(val)
FROM protocol
WHERE event = 'PROPOSE' AND nodeId = 0
GROUP BY msec
-- (msec, avg[req])

-- Client slack over time, all clients
-- export client-slack.csv
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'CLIENT_SLACK'))/1000 as msec, sum(latency)/1000
FROM request
WHERE event = 'CLIENT_SLACK'
GROUP BY msec
-- (msec, slack[ms])

-- Client slack over time, client 1
-- export client-slack-client-1.csv
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'CLIENT_SLACK' AND nodeId = -1)) as usec, latency
FROM request
WHERE event = 'CLIENT_SLACK' AND nodeId = -1
-- (usec, slack[us])

-- Client slack over time, client 2
-- export client-slack-client-2.csv
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'CLIENT_SLACK' AND nodeId = -2)) as usec, latency
FROM request
WHERE event = 'CLIENT_SLACK' AND nodeId = -2
-- (usec, slack[us])

-- Message batch size histogram
-- export histogram-msg-batch-size.csv
--
SELECT val, count()
FROM protocol_truncated
WHERE event = 'MSG_BATCH'
GROUP BY val
--(batchsize[req], count[batches])

-- Message batch sizes over time
-- export timeline-avg-msg-batch.csv
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'MSG_BATCH'))/1000 as msec, avg(val)
from protocol
WHERE event = 'MSG_BATCH'
GROUP BY msec
-- (msec, avgbatch[req])

-- Bandwidths between all pairs of peers.
-- The output of this query is meant for further processing by another scripts that formats the data as a table.
-- export bandwidths-list.csv
SELECT nodeId as sender, seqNr as receiver, val as bandwidth
FROM protocol
WHERE event = 'BANDWIDTH'
-- (sender, receiver, bandwidth[kB/s])

-- Leaders in epochs.
-- export epoch-leaders.csv
SELECT seqNr as epoch, avg(val) as leaders
FROM protocol
WHERE event = 'NEW_EPOCH'
group by epoch

-- View changes, from view 0 to view 1 only.
-- export timeline-view-change-first.csv
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'NEW_EPOCH'))/1000 as msec, count()
from protocol
WHERE event = 'VIEW_CHANGE' AND val = 1
GROUP BY msec

-- Cascading view changes.
-- export timeline-view-change-cascade.csv
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'NEW_EPOCH'))/1000 as msec, count()
from protocol
WHERE event = 'VIEW_CHANGE' AND val > 1
GROUP BY msec

-- All view changes.
-- export timeline-view-change-all.csv
SELECT (ts - (SELECT min(ts) FROM protocol WHERE event = 'NEW_EPOCH'))/1000 as msec, count()
from protocol
WHERE event = 'VIEW_CHANGE'
GROUP BY msec
