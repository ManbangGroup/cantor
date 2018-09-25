#!/bin/bash

nohup ./entrypoint.sh &
sleep 15

result_flag="does not exist"
echo "##check the table ##"
checkout=`hbase shell << EOF
exists 'infra_pub:id-gen-meta'
EOF`
echo "the checkout is ${checkout}"

function create_table () {
hbase shell << EOF
create_namespace 'infra_pub'
create 'infra_pub:id-gen-meta', {NAME => 'inst', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-0', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-1', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-2', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-3', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-4', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-5', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-6', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-7', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-8', {NAME => 'svc', REPLICATION_SCOPE => 1}
create 'infra_pub:id-gen-9', {NAME => 'svc', REPLICATION_SCOPE => 1}
EOF
}

result=$(echo $checkout | grep "${result_flag}")
if [[ "$result" != "" ]]
then
  echo "not exist, need to create table"
  create_table
else
  echo "exist ,not need to create table"
fi

echo "##complete init cantor tables##"
tail -f /hbase/logs/*