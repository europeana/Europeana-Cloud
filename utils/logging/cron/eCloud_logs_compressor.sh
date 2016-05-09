#!/bin/bash
#
# Will compress all files specified in 'fileNamesForCompression' array created the day before.
# For example for mcs.log file mcs.log.2016-02-10 will be compressed to mcs.log.2016-02-10.gz
#

#exit script of first failed command
#set -e
#forbid using unset variables in bash script
set -u

logsDirectory=/home/tomcat_user/logs/
fileNamesForCompression=( aas uis mcs dls dps iiif)
daysBack=${1:-1}

echo "Compressing log files for directory: "$logsDirectory
echo "Days back: "$daysBack

for ((i=1;i<=$daysBack;i++)); do
        date=`date -u --date="$i days ago" +"%Y-%m-%d"`
        echo "Compressing logs for date: "$date

                for j in "${fileNamesForCompression[@]}"
                do
                        filePath=$logsDirectory$j.log.$date
                        echo "Compressing file:"$filePath
                        gzip $filePath
                done
done