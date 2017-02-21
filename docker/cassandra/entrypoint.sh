#!/bin/bash
set -u
set -e

MAIN_DIR=$1
BIN=$MAIN_DIR/bin

function wait_for_cassandra_bootup () {
    until $BIN/cqlsh -u cassandra -p cassandra \
        -e "DESCRIBE system.schema_columnfamilies;" &> /dev/null
    do
        echo "."
        sleep 1s;
    done
}

if [ ! -e /cassandrac.pid ]; then
    echo "Configuring cassandra..."
    /etc/cassandra/configure.sh $MAIN_DIR
    touch /cassandrac.pid
fi

echo "Starting services..."
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

wait_for_cassandra_bootup

if [ ! -e /cassandrak.pid ]; then
    echo "Initializing keyspces..."
    /etc/cassandra/initialize_tables.sh $MAIN_DIR
    touch /cassandrak.pid
fi

ls -l /var/log/supervisor/cassandra.log && tail -f /var/log/supervisor/cassandra.log