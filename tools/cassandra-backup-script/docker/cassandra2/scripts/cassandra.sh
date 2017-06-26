#!/bin/bash
#
# /etc/init.d/cassandra
#
# Startup script for Cassandra
#

case "$1" in
    start)
        # Cassandra startup
        echo -n "Starting Cassandra: "
	    /bin/bash -c "/opt/apache-cassandra-2.1.8/bin/cassandra" 2>&1 &> /dev/null
        echo "OK"
        ;;
    stop)
        # Cassandra shutdown
        echo -n "Shutdown Cassandra: "
        pkill -f org.apache.cassandra.service.CassandraDaemon
        echo "OK"
        ;;
    *)
        echo "Usage: `basename $0` start|stop"
        exit 1
esac

exit 0
