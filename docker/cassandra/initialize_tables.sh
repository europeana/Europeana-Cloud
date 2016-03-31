#!/bin/bash

/opt/apache-cassandra-2.1.8/bin/cassandra
sleep 20s
/opt/apache-cassandra-2.1.8/bin/cqlsh -u cassandra -p cassandra -f /etc/cassandra/users.cql
/opt/apache-cassandra-2.1.8/bin/cqlsh -u cassandra -p cassandra -f /etc/cassandra/uis_setup.cql
/opt/apache-cassandra-2.1.8/bin/cqlsh -u cassandra -p cassandra -f /etc/cassandra/mcs_setup.cql
/opt/apache-cassandra-2.1.8/bin/cqlsh -u cassandra -p cassandra -f /etc/cassandra/aas_setup.cql
/opt/apache-cassandra-2.1.8/bin/cqlsh -u cassandra -p cassandra -f /etc/cassandra/dps_setup.cql
/opt/apache-cassandra-2.1.8/bin/cqlsh -u cassandra -p cassandra -f /etc/cassandra/aas_dml.cql
sleep 20s
pkill -f CassandraDaemon
