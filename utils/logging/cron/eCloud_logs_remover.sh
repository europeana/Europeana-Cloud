#!/bin/bash
#
# Will remove all old log files specified in 'fileNamesForDeletion' array.
#

#exit script of first failed command
#set -e
#forbid using unset variables in bash script
set -u

logsDirectory=/var/log/ecloud/
fileNamesForDeletion=(uis mcs dps)
daysBack=${1:-20}

echo "$(date) Deleting log files for directory: "$logsDirectory
echo "Days back: "$daysBack

date=`date -u --date="$daysBack days ago" +"%Y-%m-%d"`
echo "Deleting logs for date: "$date

for j in "${fileNamesForDeletion[@]}"
do
        filePath=$logsDirectory$j/$j.log.$date-*.log.gz
        echo "Deleting file:"$filePath
        rm $filePath
done
