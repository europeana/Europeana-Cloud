#!/bin/bash
set -s
set -e

MAIN_DIR=$1
BIN=$MAIN_DIR/bin

$BIN/cassandra
sleep 20s
$BIN/cqlsh -u cassandra -p cassandra -f /etc/cassandra/db_users.cql
$BIN/cqlsh -u cassandra -p cassandra -f /etc/cassandra/uis_setup.cql
$BIN/cqlsh -u cassandra -p cassandra -f /etc/cassandra/mcs_setup.cql
$BIN/cqlsh -u cassandra -p cassandra -f /etc/cassandra/aas_setup.cql
$BIN/cqlsh -u cassandra -p cassandra -f /etc/cassandra/dps_setup.cql
$BIN/cqlsh -u cassandra -p cassandra -f /etc/cassandra/aas_users.cql
sleep 5s
pkill -f CassandraDaemon
