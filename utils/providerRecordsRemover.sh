#!/bin/bash

#Script that will remove all record associated with given provider.
#Admin credentials are required here
#
#Usage:
#./script.sh <ecloudLocation> <adminName> <adminPassword> <mcsPrefix> <uisPrefix> <providerName>
#./script.sh http://127.0.0.1:8080 admin admin mcs uis Provider___%202016_03_24_13_50_28_4_453
#
#

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
#wildcard *
shopt -s nullglob

ecloudLocation=$1
adminName=$2
adminPass=$3
mcsPrefix=$4
uisPrefix=$5
providerId=$6

echo Reading cloudIds for given provider id: $providerId
existingCloudIds=`curl -s --insecure $ecloudLocation/$uisPrefix/data-providers/$providerId/cloudIds | xmllint --xpath "/resultSlice/results/id" -`

existingCloudIds=$(echo $existingCloudIds | sed -e 's|</id><id>|\n|g')
existingCloudIds=$(echo $existingCloudIds | sed -e 's|<id>|''|g')
existingCloudIds=$(echo $existingCloudIds | sed -e 's|</id>|''|g')

array=($existingCloudIds)
echo Found ${#array[@]} cloudIds

for ((i=0; i < ${#array[@]}; i++))
do
        echo Removing representations for cloudId: ${array[$i]}
        curl --user $adminName:$adminPass --insecure -X DELETE $ecloudLocation/$mcsPrefix/records/${array[$i]}
        echo Representations removed
        echo Marking cloudId as deleted ${array[$i]}
        curl --user $adminName:$adminPass --insecure -X DELETE $ecloudLocation/$uisPrefix/cloudIds/${array[$i]}
        echo CloudId marked as deleted
done
