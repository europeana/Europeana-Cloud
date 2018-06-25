#!/bin/bash
#
# Will clean directoyry with kafka logs:
# 1. Compress all files specified in 'fileNamesForProcessing' array created #{daysBackForCompression} days before.
# 2. Delete all files specified in 'fileNamesForProcessing' array created #{daysBackForDeletion} days before.
#server.log.2018-02-16-00
# For example for server.log:
# - file server.log.2018-02-16-00 will be compressed to server.log.2018-02-16-00.gz
# - file server.log.2018-02-10-00.gz will be removed


#exit script of first failed command
#set -e
#forbid using unset variables in bash script
set -u

#PARAMETERS
logsDirectory=logs/
fileNamesForProcessing=( server.log state-change.log controller.log)
daysBackForCompression=5
daysBackForDeletion=10
########

echo "Kafka logs cleaner execution"
echo "Compressing log files for directory: "$logsDirectory

date=`date -u --date="$daysBackForCompression days ago" +"%Y-%m-%d"`

for j in "${fileNamesForProcessing[@]}"
do
        filePath=$logsDirectory$j.$date-*
        echo "Compressing file:"$filePath
        gzip $filePath
done

echo "Deteting log files for directory: "$logsDirectory

date=`date -u --date="$daysBackForDeletion days ago" +"%Y-%m-%d"`
echo $date
for j in "${fileNamesForProcessing[@]}"
do
        filePath=$logsDirectory$j.$date-*".gz"
        echo "Deleting file:"$filePath
        rm $filePath
done
