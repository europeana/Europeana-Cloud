#!/bin/bash

#Script that will remove all record associated with given provider.
#Admin credentials are required here
#
#Usage:
#./script.sh <providerName> <adminName> <adminPassword>
#./script.sh Provider___%202016_03_24_13_50_28_4_453 admin admin
#
#

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
#wildcard *
shopt -s nullglob

providerId=$1
adminName=$2
adminPass=$3

ecloudLocation=http://127.0.0.1:8080
mcsPrefix=mcs
uisPrefix=uis

echo Reading cloudIds for given provider id: $providerId
existingCloudIds=`curl -s $ecloudLocation/$uisPrefix/data-providers/$providerId/cloudIds | xmllint --xpath "/resultSlice/results/id" -`

existingCloudIds=$(echo $existingCloudIds | sed -e 's|</id><id>|\n|g')
existingCloudIds=$(echo $existingCloudIds | sed -e 's|<id>|''|g')
existingCloudIds=$(echo $existingCloudIds | sed -e 's|</id>|''|g')

array=($existingCloudIds)
echo Found ${#array[@]} cloudIds

for ((i=0; i < ${#array[@]}; i++))
do
        echo Removing representations for cloudId: ${array[$i]}
        curl --user $adminName:$adminPass -X DELETE $ecloudLocation/$mcsPrefix/records/${array[$i]}
        echo Representations removed
        echo Marking cloudId as deleted ${array[$i]}
        curl --user $adminName:$adminPass -X DELETE $ecloudLocation/$uisPrefix/cloudIds/${array[$i]}
        echo CloudId marked as deleted
done
