#!/bin/bash

#delete specified solr collection

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
shopt -s nullglob

#check number of parameters
if [ $# -ne 1 ]
then
	echo usage: "delete-collection.sh <collection-name>"
	exit -1
fi

collection=$1
SOLR_HTTP='http://localhost:8080/solr'
CALL="$SOLR_HTTP/admin/collections?action=DELETE&name=$collection"

#responseCode=`curl -s -o /dev/null "$CALL" -H -d -I -w '%{http_code}'`
curl "$CALL" -H -d 

#if [ $responseCode -ne 200 ]
#then
#	exit 1
#fi
#
#exit 0
