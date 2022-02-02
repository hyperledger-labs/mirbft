-- Truncate request table to include only rows with timestamps between:
--   the first response obtained by the last client to obtain a response
--   and
--   the last response obtained by the client first to finish obtaining responses
-- Then, cut off 1 second from each end.
-- We could use a view here too, but a physical table is much faster to access.
-- The script processing this file normally makes sure that
-- changes made by this script to the database are rolled back and are not persisted.
CREATE TABLE request_truncated as
SELECT *
FROM request
WHERE
  ts - 1000000 >= (SELECT max(t)
    FROM (SELECT min(ts) as t
      FROM request
      WHERE event = 'REQ_FINISHED'
      GROUP BY clId))
  AND ts + 1000000 <= (SELECT min(t)
    FROM (SELECT max(ts) as t
      FROM request
      WHERE event = 'REQ_FINISHED'
      GROUP BY clId))

-- End-to-end request latency, by client
--
SELECT clId, count(), avg(r2.ts - r1.ts)/1000.0, stdev(r2.ts - r1.ts)/1000.0
FROM request r1 JOIN request r2 USING(clId, clSn)
WHERE r1.event = 'REQ_SEND' AND r2.event = 'REQ_FINISHED'
GROUP BY clId
-- (clId, count[req], avg[ms], stdev[ms])

-- Slow requests (requests with more than 5x the average latency)
--
SELECT clId, clSn, (r2.ts - r1.ts)/1000
FROM request r1 JOIN request r2 USING(clId, clSn)
WHERE r1.event = 'REQ_SEND' AND r2.event = 'REQ_FINISHED' AND (r2.ts - r1.ts) > 5 * (SELECT avg(sr2.ts - sr1.ts)
  FROM request sr1 JOIN request sr2 USING(clId, clSn)
  WHERE sr1.event = 'REQ_SEND' AND sr2.event = 'REQ_FINISHED')
-- (clId, clSn, latency[ms])

-- Throughput in time for client 0 (per millisecond)
-- export throughput-in-time-client-0.csv
--
SELECT (ts - (SELECT min(ts) FROM request WHERE event = 'REQ_FINISHED' AND clId = 0))/1000 as msec, count()
FROM request
WHERE event = 'REQ_FINISHED' AND clId = 0
GROUP BY msec
-- (msec, count[req])

-- Throughput (raw) per client
--
SELECT clID, count(), (max(ts) - min(ts))/1000000.0, 1000000.0*count() / (max(ts) - min(ts))
FROM request
WHERE event = 'REQ_FINISHED'
GROUP BY clId
-- (clId, count[req], time[sec], throughput[req/sec])

-- Throughput, using truncated request data, per client
--
SELECT clID, count(), (max(ts) - min(ts))/1000000.0, 1000000.0*count() / (max(ts) - min(ts))
FROM request_truncated
WHERE event = 'REQ_FINISHED'
GROUP BY clId
-- (clId, count[req], time[sec], throughput[req/sec])

