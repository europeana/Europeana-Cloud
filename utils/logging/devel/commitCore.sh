#!/bin/bash

#issue commit for solr core specified as first argument

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
shopt -s nullglob

if [ $# -ne 1 ]
then
	echo usage: "commitCore.sh <collection-name>"
	exit -1
fi

CORE=$1
SOLR_HTTP='http://localhost:8080/solr'
CALL="$SOLR_HTTP/$CORE/update?commit=true"

responseCode=`curl -s -o /dev/null "$CALL" -H -d -I -w '%{http_code}'`

if [ $responseCode -ne 200 ]
then
	exit 1
fi

exit 0
