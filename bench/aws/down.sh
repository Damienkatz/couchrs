#!/usr/bin/env bash
# Tear down everything aws/ created: the instances tagged
# Project=couchdb-rs-bench, then the placement group, security group and
# key pair once the instances are gone.
set -uo pipefail
ids=$(aws ec2 describe-instances --filters Name=tag:Project,Values=couchdb-rs-bench \
  Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -n "$ids" ]; then
  echo "terminating: $ids"
  aws ec2 terminate-instances --instance-ids $ids >/dev/null
  aws ec2 wait instance-terminated --instance-ids $ids
fi
aws ec2 delete-placement-group --group-name couchdb-rs-bench 2>/dev/null && echo "placement group deleted"
sg=$(aws ec2 describe-security-groups --filters Name=group-name,Values=couchdb-rs-bench --query 'SecurityGroups[0].GroupId' --output text)
[ "$sg" != "None" ] && aws ec2 delete-security-group --group-id "$sg" && echo "security group deleted"
aws ec2 delete-key-pair --key-name couchdb-rs-bench-damien && echo "key pair deleted"
