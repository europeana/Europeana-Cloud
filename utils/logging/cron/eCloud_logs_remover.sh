#!/bin/bash
#
# Will remove all old log files specified in 'fileNamesForDeletion' array.
#

#exit script of first failed command
#set -e
#forbid using unset variables in bash script
set -u

logsDirectory=/home/tomcat_user/logs/
fileNamesForDeletion=( aas uis mcs dps iiif)
daysBack=${1:-20}

echo "Deleting log files for directory: "$logsDirectory
echo "Days back: "$daysBack

date=`date -u --date="$daysBack days ago" +"%Y-%m-%d"`
echo "Deleting logs for date: "$date

for j in "${fileNamesForDeletion[@]}"
do
        filePath=$logsDirectory$j.log.$date.gz
        echo "Deleting file:"$filePath
        rm $filePath
done
