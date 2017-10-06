# `pg_prefaulter` validation

This validation program is intended to verify that IO requested by the
`pg_prefaulter` is later `pread(2)` by PostgreSQL.  The following is an example
procedure:

1. Create a new primary database: `make freshdb-primary`
2. Create a new follower: `make freshdb-follower`
3. Start `pg_prefaulter` on the follower `pg_prefaulter`
4. Start DTrace script and redirect the output to a file:
   `dtrace -s ../scripts/iotrace.d > iotrace.out`
5) Initialize a `pgbench` test: `make pgbench-init`
6) Run `pgbench`: `make pgbench`
7) Terminate the `iotrace.d` DTrace scrit
8) Split the `iotrace.d` data files into `pg_prefaulter` and non-`pg_prefaulter`
   traces:
   a) `grep pg_prefaulter iotrace.out > iotrace.pg_prefaulter`
   b) `grep -v pg_prefaulter iotrace.out > iotrace.postgres`
9) Run the experiment validation script: `go run validate.go`

The last line of output should include various stats indicating the efficacy
of `pg_prefaulter` for the given workload (i.e. pgbench).

## Sample Output

```
-bash-4.3$ make pgbench
2>&1 env PGSSLMODE=disable PGHOST=/tmp PGUSER=postgres PGPASSWORD="`cat \".pwfile\"`" "/opt/local/bin/pgbench" -j 64 -P 60 -r -T 400 --no-vacuum --protocol=prepared
progress: 60.0 s, 113.4 tps, lat 8.790 ms stddev 35.131
progress: 120.0 s, 333.8 tps, lat 2.995 ms stddev 4.557
progress: 180.0 s, 329.5 tps, lat 3.033 ms stddev 4.989
progress: 240.0 s, 317.5 tps, lat 3.147 ms stddev 4.866
progress: 300.0 s, 291.1 tps, lat 3.433 ms stddev 5.311
progress: 360.0 s, 286.2 tps, lat 3.490 ms stddev 12.019
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 10
query mode: prepared
number of clients: 1
number of threads: 1
duration: 400 s
number of transactions actually processed: 113901
latency average = 3.509 ms
latency stddev = 10.804 ms
tps = 284.713903 (including connections establishing)
tps = 284.820141 (excluding connections establishing)
script statistics:
 - statement latencies in milliseconds:
         0.003  \set aid random(1, 100000 * :scale)
         0.001  \set bid random(1, 1 * :scale)
         0.001  \set tid random(1, 10 * :scale)
         0.001  \set delta random(-5000, 5000)
         0.373  BEGIN;
         0.383  UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
         0.721  SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
         0.492  UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
         0.511  UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
         0.587  INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
         0.423  END;
```

```
$ grep pg_prefaulter iotrace.pgbench > iotrace.pg_prefaulter
$ grep -v pg_prefaulter iotrace.pgbench > iotrace.postgres
$ go run validate.go | tail
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16384,"segment":0,"offset":12460032,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16384,"segment":0,"offset":12574720,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16399,"segment":0,"offset":81920,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16384,"segment":0,"offset":12124160,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16399,"segment":0,"offset":16384,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16387,"segment":0,"offset":450560,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16384,"segment":0,"offset":13508608,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16384,"segment":0,"offset":13672448,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"debug","relation":16384,"segment":0,"offset":13754368,"message":"prefaulter faulted a page that PostgreSQL never read in"}
{"time":"2017-10-05T17:40:49-07:00","level":"info","prefaulted-page":22975,"prefaulted-unused-page":297,"pct-hit-rate":"1.293%","message":"unprefaulter faulted a page that PostgreSQL never read in (or hadn't read in yet)"}
```
