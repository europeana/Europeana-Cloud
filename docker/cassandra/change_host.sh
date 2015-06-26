#!/bin/bash

IP=`hostname --ip-address`
CONFIG=/opt/apache-cassandra-2.0.6/conf

sed -i -e "s/^listen_address.*/listen_address: $IP/"            $CONFIG/cassandra.yaml
sed -i -e "s/^rpc_address.*/rpc_address: 0.0.0.0/"              $CONFIG/cassandra.yaml
sed -i -e "s/# broadcast_address.*/broadcast_address: $IP/"              $CONFIG/cassandra.yaml
sed -i -e "s/# broadcast_rpc_address.*/broadcast_rpc_address: $IP/"              $CONFIG/cassandra.yaml
sed -i -e "s/127.0.0.1/ $IP/" $CONFIG/cassandra.yaml