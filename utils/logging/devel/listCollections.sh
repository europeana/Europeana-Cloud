#!/bin/bash

#list collections

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
shopt -s nullglob

SOLR_HTTP='http://localhost:8080/solr'
CALL="$SOLR_HTTP/admin/collections?action=LIST"

result=(`curl -s "$CALL" -H -d | xmllint --xpath "/response/arr[@name='collections']/str" - | sed -e 's|<str>\([^<>]\+\)</str>|\1\n|g'`)

for ((i=0; i < ${#result[@]}; i++))
do
	echo ${result[$i]}
done
