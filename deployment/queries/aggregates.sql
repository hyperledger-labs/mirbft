-- Truncate request table to include only rows with timestamps between:
--   the first response obtained by the last client to obtain a response
--   and
--   the last response obtained by the client first to finish obtaining responses
-- Then, cut off 5 seconds from each end.
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


-- Total CPU usage (average over all peers), truncated data
-- export cpu-total.val
SELECT avg(total)
from cpuusage_truncated
WHERE event = 'CPU_USAGE'
-- (totalcpu[%])

-- System CPU usage (average over all peers), truncated data
-- (This is the CPU usage reported under "System" in /proc/stat)
-- export cpu-system.val
SELECT avg(system)
from cpuusage_truncated
WHERE event = 'CPU_USAGE'
-- (systemcpu[%])

-- End-to-end average request latency, all requests
-- export latency-avg-shortened-raw.val
--
SELECT avg(r2.latency)  / 1000.0
FROM request r1 JOIN request r2 ON r1.nodeId = r2.nodeId
                                AND r1.clSn = r2.clSn
                                AND r1.event = 'REQ_SEND'
                                AND r2.event = 'REQ_FINISHED'
   WHERE r1.ts - 30000000 < (SELECT min(r1.ts)
                            FROM request r1
                            WHERE event = 'REQ_SEND')
-- (avg[ms])

-- End-to-end average request latency, all requests
-- export latency-95pctile-shortened-raw.val
--
SELECT r2.latency  / 1000.0
FROM request r1 JOIN request r2 ON r1.nodeId = r2.nodeId
                                AND r1.clSn = r2.clSn
                                AND r1.event = 'REQ_SEND'
                                AND r2.event = 'REQ_FINISHED'
   WHERE r1.ts - 30000000 < (SELECT min(r1.ts)
                            FROM request r1
                            WHERE event = 'REQ_SEND')
ORDER BY r2.latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request r1 JOIN request r2 ON r1.nodeId = r2.nodeId
                                AND r1.clSn = r2.clSn
                                AND r1.event = 'REQ_SEND'
                                AND r2.event = 'REQ_FINISHED'
            WHERE r1.ts - 30000000 < (SELECT min(r1.ts)
                            FROM request r1
                            WHERE event = 'REQ_SEND'))* 95 / 100;

-- (latency[ms])

-- End-to-end average request latency, all requests
-- export latency-avg-raw.val
--
SELECT avg(latency) / 1000.0
FROM request
WHERE event = 'REQ_FINISHED'
-- (avg[ms])

-- Latency standard deviation, all requests
-- export latency-stdev-raw.val
--
SELECT stdev(latency) / 1000.0
FROM request
WHERE event = 'REQ_FINISHED'
-- (stdev[ms])

-- End-to-end average request latency, truncated requests
-- export latency-avg-trunc.val
--
SELECT avg(latency) / 1000.0
FROM request_truncated
WHERE event = 'REQ_FINISHED'
-- (avg[ms])

-- Latency standard deviation, truncated requests
-- export latency-stdev-trunc.val
--
SELECT stdev(latency) / 1000.0
FROM request_truncated
WHERE event = 'REQ_FINISHED'
-- (stdev[ms])

-- 95th percentile request latency, all requests
-- export latency-95pctile-raw.val
--
SELECT latency / 1000.0
FROM request
WHERE event = 'REQ_FINISHED'
ORDER BY latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request
            WHERE event = 'REQ_FINISHED') * 95 / 100;
-- (latency[ms])

-- 95th percentile request latency, truncated requests
-- export latency-95pctile-trunc.val
--
SELECT latency / 1000.0
FROM request_truncated
WHERE event = 'REQ_FINISHED'
ORDER BY latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request_truncated
            WHERE event = 'REQ_FINISHED') * 95 / 100;
-- (latency[ms])




-- End-to-end average request latency, all requests
-- export latency-avg-raw-nowm.val
--
SELECT avg(latency) / 1000.0
FROM request
WHERE event = 'ENOUGH_RESP'
-- (avg[ms])

-- Latency standard deviation, all requests
-- export latency-stdev-raw-nowm.val
--
SELECT stdev(latency) / 1000.0
FROM request
WHERE event = 'ENOUGH_RESP'
-- (stdev[ms])

-- End-to-end average request latency, truncated requests
-- export latency-avg-trunc-nowm.val
--
SELECT avg(latency) / 1000.0
FROM request_truncated
WHERE event = 'ENOUGH_RESP'
-- (avg[ms])

-- Latency standard deviation, truncated requests
-- export latency-stdev-trunc-nowm.val
--
SELECT stdev(latency) / 1000.0
FROM request_truncated
WHERE event = 'ENOUGH_RESP'
-- (stdev[ms])

-- 95th percentile request latency, all requests
-- export latency-95pctile-raw-nowm.val
--
SELECT latency / 1000.0
FROM request
WHERE event = 'ENOUGH_RESP'
ORDER BY latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request
            WHERE event = 'ENOUGH_RESP') * 95 / 100;
-- (latency[ms])

-- 95th percentile request latency, truncated requests
-- export latency-95pctile-trunc-nowm.val
--
SELECT latency / 1000.0
FROM request_truncated
WHERE event = 'ENOUGH_RESP'
ORDER BY latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request_truncated
            WHERE event = 'ENOUGH_RESP') * 95 / 100;
-- (latency[ms])





-- Average client slack per request, all clients
-- export client-slack-avg-raw.val
--
SELECT avg(latency)
FROM request
WHERE event = 'CLIENT_SLACK'
-- (avg[us])

-- Average client slack, truncated requests
-- export client-slack-avg-trunc.val
--
SELECT avg(latency)
FROM request_truncated
WHERE event = 'CLIENT_SLACK'
-- (avg[us])

-- Client slack standard deviation, all requests
-- export client-slack-stdev-raw.val
--
SELECT stdev(latency)
FROM request
WHERE event = 'CLIENT_SLACK'
-- (avg[us])

-- Client slack standard deviation, all requests
-- export client-slack-stdev-trunc.val
--
SELECT stdev(latency)
FROM request_truncated
WHERE event = 'CLIENT_SLACK'
-- (avg[us])

-- 1st percentile client slack, all requests
-- export client-slack-1pctile-raw.val
--
SELECT latency
FROM request
WHERE event = 'CLIENT_SLACK'
ORDER BY latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request
            WHERE event = 'CLIENT_SLACK') * 1 / 100;
-- (slack[us])

-- 1st percentile client slack, truncated requests
-- export client-slack-1pctile-trunc.val
--
SELECT latency
FROM request_truncated
WHERE event = 'CLIENT_SLACK'
ORDER BY latency ASC
LIMIT 1
    OFFSET (SELECT count()
            FROM request
            WHERE event = 'CLIENT_SLACK') * 1 / 100;
-- (slack[us])

-- Experiment duration in seconds
-- export duration-raw.val
--
SELECT (max(ts) - min(ts))/1000000.0
FROM request
WHERE event = 'REQ_FINISHED'
-- (duration[sec])

-- Experiment duration in seconds, truncated data
-- export duration-trunc.val
--
SELECT (max(ts) - min(ts))/1000000.0
FROM request_truncated
WHERE event = 'REQ_FINISHED'
-- (duration[sec])

-- Total number of requests (good for sanity checks whether everything has been delivered)
-- export nreq-raw.val
--
SELECT count()
FROM request
WHERE event = 'REQ_FINISHED'
-- (count[req])

-- Number of requests, using truncated request data
-- export nreq-trunc.val
--
SELECT count()
FROM request_truncated
WHERE event = 'REQ_FINISHED'
-- (count[req])

-- Throughput (without cutting off start and end)
-- export throughput-raw.val
--
-- !!!!! Multiplying by 10 for sampling
-- TODO parametrize the sumpling multiplier
SELECT 10 * 1000000.0 * count() / (max(ts) - min(ts))
FROM request
WHERE event = 'REQ_FINISHED'
-- (throughput[req/sec])

-- Throughput, using truncated request data
-- export throughput-trunc.val
--
-- !!!!! Multiplying by 10 for sampling
-- TODO parametrize the sumpling multiplier
SELECT 10 * 1000000.0 * count() / (max(ts) - min(ts))
FROM request_truncated
WHERE event = 'REQ_FINISHED'
-- (throughput[req/sec])

-- Average batch size
-- export batch-size-avg-trunc.val
--
SELECT avg(val)
FROM protocol_truncated
WHERE event = 'PROPOSE'
-- (avg-batch-size[req])

-- 10th percentile batch size
-- export batch-size-10pctile-trunc.val
--
SELECT val
FROM protocol_truncated
WHERE event = 'PROPOSE'
ORDER BY val ASC
LIMIT 1
OFFSET (SELECT count() from protocol_truncated WHERE event = 'PROPOSE') * 10 / 100
-- (10th-pctile[req])

-- 90th percentile batch size
-- export batch-size-90pctile-trunc.val
--
SELECT val
FROM protocol_truncated
WHERE event = 'PROPOSE'
ORDER BY val ASC
LIMIT 1
    OFFSET (SELECT count() from protocol_truncated WHERE event = 'PROPOSE') * 90 / 100
-- (90th-pctile[req])

-- average proposal rate
-- export propose-rate-raw.val
SELECT count(*) / ((max(ts) - min(ts))/1000000.0)
FROM protocol
WHERE event = 'PROPOSE'
-- (rate[batches/sec])

-- average proposal rate, truncated data
-- export propose-rate-trunc.val
SELECT count(*) / ((max(ts) - min(ts))/1000000.00)
FROM protocol_truncated
WHERE event = 'PROPOSE'
-- (rate[batches/sec])

-- average commit rate
-- export commit-rate-raw.val
SELECT count(*) / ((max(ts) - min(ts))/1000000.0)
FROM protocol
WHERE event = 'COMMIT'
-- (rate[batches/sec])

-- average commit rate, truncated data
-- export commit-rate-trunc.val
SELECT count(*) / ((max(ts) - min(ts))/1000000.0)
FROM protocol_truncated
WHERE event = 'COMMIT'
-- (rate[batches/sec])

-- average message batch, truncated data
-- export msg-batch-avg-trunc.val
SELECT avg(val)
FROM protocol_truncated
where event = 'MSG_BATCH'
-- (batchsize[msg])

-- minimal number of epochs
-- this is the number of epochs the node with the fewest epochs went through (ideally same for all nodes)
-- export epochs-min.val
SELECT min(lastEpoch)
FROM (SELECT max(seqNr) as lastEpoch
      FROM protocol
      WHERE event = 'NEW_EPOCH'
      GROUP BY nodeId)

-- maximal number of epochs
-- this is the number of epochs the node with the fewest epochs went through (ideally same for all nodes)
-- export epochs-max.val
SELECT max(lastEpoch)
FROM (SELECT max(seqNr) as lastEpoch
      FROM protocol
      WHERE event = 'NEW_EPOCH'
      GROUP BY nodeId)

-- maximal number of epochs
-- this is the number of epochs the node with the fewest epochs went through (ideally same for all nodes)
-- export epochs-avg.val
SELECT avg(lastEpoch)
FROM (SELECT max(seqNr) as lastEpoch
      FROM protocol
      WHERE event = 'NEW_EPOCH'
      GROUP BY nodeId)

-- Total number of view changes at all nodes
-- export viewchanges-total.val
SELECT count()
FROM protocol
WHERE event = 'VIEW_CHANGE'

-- Average number of view changes (should be the total number of actual view changes, if every node completes each view change)
-- export viewchanges-avg.val
SELECT count()*1.0 / (SELECT count() from (SELECT DISTINCT nodeId FROM protocol))
FROM protocol
WHERE event = 'VIEW_CHANGE'
