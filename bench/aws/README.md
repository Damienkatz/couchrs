# Concurrent benchmark on AWS

Four pairs, each a server (i4i.4xlarge, data on local NVMe) and its own
load generator (c7i.4xlarge with 5 secondary private IPs, the scaling
test's six "hosts"), all in one AZ and one cluster placement group:
CouchDB 3.5.3 (Docker), couchdb-rs `main` (B-tree), `rocksdb-experiment`
(hybrid) and `async-blocking` (hybrid, point reads inline).

1. Launch (us-east-1a, Amazon Linux 2023, default VPC), tagging
   everything `Project=couchdb-rs-bench`: import a key pair
   `couchdb-rs-bench-damien`, create security group `couchdb-rs-bench`
   (SSH from your IP, all traffic within the group) and cluster placement
   group `couchdb-rs-bench`, then `aws ec2 run-instances` 4× i4i.4xlarge
   and 4× c7i.4xlarge with
   `--network-interfaces DeviceIndex=0,...,SecondaryPrivateIpAddressCount=5`
   (load generators only). Name them `couchdb-bench-{srv,lg}-{erlang,btree,hybrid,inline}`
   and write `hosts.txt`: `NAME PUBLIC_IP PRIVATE_IP ALL_PRIVATE_IPS` per line.
2. `prep.sh server|lg|builder` on each node (as root): kernel and fd
   limits, NVMe on /data and Docker on servers, toolchain on the builder.
3. Build the three couchdb-rs branches and couchbench on the builder;
   copy `couchdb-rs-<kind>` and `srvctl.sh`/`monitor.py` to each server's
   `~/bin`, and `couchbench`, `couchscale`, `pair.sh`, `db_fingerprint.py`
   to each load generator. Give each load generator an SSH key for its
   own server.
4. On every load generator at once:
   `nohup ./pair.sh NAME KIND SERVER_PRIVATE_IP HOST_IPS > run.log &`
   (`TOTAL_GB` and `SCALE_ARGS` shrink it for a rehearsal).
5. `collect.py` fetches and merges the results into `results-aws.json`;
   `REPORT_ENV=aws ../report.py ../results-aws.json ../report-aws.html`.
6. `down.sh` terminates everything tagged `Project=couchdb-rs-bench` and
   deletes the placement group, security group and key pair.

`hosts.txt`, `nodes.txt` and `state.env` are per-run state (addresses,
resource ids) and are not committed.
