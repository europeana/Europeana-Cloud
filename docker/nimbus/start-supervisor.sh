#!/bin/bash
export IP=`hostname -i`

echo "storm.zookeeper.servers:" > $STORM_HOME/conf/storm.yaml
echo "    - \"docker_zookeeper_1\"" >> $STORM_HOME/conf/storm.yaml
echo "nimbus.host: \"$IP\"" >> $STORM_HOME/conf/storm.yaml
echo "drpc.servers:" >> $STORM_HOME/conf/storm.yaml
echo "  - \"$IP\"" >> $STORM_HOME/conf/storm.yaml
echo "drpc.port: 3772" >> $STORM_HOME/conf/storm.yaml
echo "drpc.invocations.port: 3773" >> $STORM_HOME/conf/storm.yaml
echo "storm.local.hostname: \"$IP\"" >> $STORM_HOME/conf/storm.yaml

/usr/sbin/sshd && supervisord
