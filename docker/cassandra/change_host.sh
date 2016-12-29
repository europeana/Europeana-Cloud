#!/bin/bash
set -s
set -e

IP=`hostname --ip-address`
CONFIG=/opt/apache-cassandra-2.1.8/conf

sed -i -e "s/^listen_address.*/listen_address: localhost/"            $CONFIG/cassandra.yaml
sed -i -e "s/^rpc_address.*/rpc_address: 0.0.0.0/"              $CONFIG/cassandra.yaml
sed -i -e "s/# broadcast_rpc_address.*/broadcast_rpc_address: $IP/"              $CONFIG/cassandra.yaml
