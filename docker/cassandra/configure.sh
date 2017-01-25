#!/bin/bash
set -s
set -e

IP=`hostname --ip-address`
MAIN_DIR=$1
CONFIG=$MAIN_DIR/conf

#create dirs
mkdir -p /data/cassandra/datafile
mkdir -p /data/cassandra/commitlog
mkdir -p /data/cassandra/caches

# point to the above directories in yaml file
sed -i 's/\/var\/lib\/cassandra\/data/\/data\/cassandra\/datafile/' $CONFIG/cassandra.yaml 
sed -i 's/\/var\/lib\/cassandra\/commitlog/\/data\/cassandra\/commitlog/' $CONFIG/cassandra.yaml 
sed -i 's/\/var\/lib\/cassandra\/saved_caches/\/data\/cassandra\/caches/' $CONFIG/cassandra.yaml

# set authenticator
sed -i 's/authenticator: AllowAllAuthenticator/authenticator: PasswordAuthenticator/' $CONFIG/cassandra.yaml
sed -i 's/authorizer: AllowAllAuthorizer/authorizer: CassandraAuthorizer/' $CONFIG/cassandra.yaml

#change ports
sed -i -e "s/^listen_address.*/listen_address: localhost/"            $CONFIG/cassandra.yaml
sed -i -e "s/^rpc_address.*/rpc_address: 0.0.0.0/"              $CONFIG/cassandra.yaml
sed -i -e "s/# broadcast_rpc_address.*/broadcast_rpc_address: $IP/"              $CONFIG/cassandra.yaml

# fix for issue http://stackoverflow.com/questions/36867832/cassandra-2-0-and-later-require-java-7u25-or-later-but-im-using-jdk1-7-0-101
sed -i -e "s/< \"25\"/< \"0\"/" $CONFIG/cassandra-env.sh
