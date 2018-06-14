#!/bin/bash
set -u
set -e

MAIN_DIR=$1


cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -f /etc/cassandra/db_users.cql;
cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -f /etc/cassandra/aas_setup.cql;
cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -f /etc/cassandra/uis_setup.cql;
cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -f /etc/cassandra/mcs_setup.cql;
cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -f /etc/cassandra/dps_setup.cql;
cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -f /etc/cassandra/aas_users.cql;
cqlsh --cqlversion=3.2.0 -u cassandra -p cassandra -e 'select * from ecloud_aas.users;';
