#!/bin/bash

#list collections

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
shopt -s nullglob

SOLR_HTTP='http://localhost:8080/solr'
selectColCall="$SOLR_HTTP/admin/cores?action=STATUS"
existingColsOutput=`curl -s "$selectColCall" -H -d | xmllint --xpath "/response/lst[@name='status']/lst/@name" -`
existingCols=(`echo $existingColsOutput | sed -e 's|name="\([^"]\+\)"|\1\n|g'`)

for ((i=0; i < ${#existingCols[@]}; i++))
do
	echo ${existingCols[$i]}
done

