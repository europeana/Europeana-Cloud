#!/bin/bash


LOGSTASH_HOME=/home/$USER/install/logstash/logstash
PLUGIN_PATH=/home/$USER/ecloud/repo/utils/logging/plugins

CONF_FILE=ecloud.config

if [ $# -gt 0 ]
then
	CONF_FILE=$1
fi

CONF_FILE_PATH=/home/$USER/ecloud/repo/utils/logging/logstashConf/$CONF_FILE

$LOGSTASH_HOME/bin/logstash --pluginpath $PLUGIN_PATH -f $CONF_FILE_PATH
