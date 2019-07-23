#!/bin/bash
#
# Will compress all files matching regex '*{date}.log' in 'logsDirectories' directories.
# For now data is equals to yesterday.
#

#exit script of first failed command
#set -e
#forbid using unset variables in bash script
set -u

readonly logFile="/home/centos/bin/filesCompression.log"

logsDirectories=( 
	/home/centos/eCloudLogs/mcs/ 
	/home/centos/eCloudLogs/uis/
	/home/centos/eCloudLogs/dps/
	/home/centos/eCloudLogs/aas/ )
daysBack=${1:-1}

echo "`date '+%Y-%m-%d %H:%M:%S'` Files compression started" >> $logFile
echo "Days back: "$daysBack >> $logFile

touch $logFile
for logsDirectory in "${logsDirectories[@]}"
do
	echo "Compressing log files for directory: "$logsDirectory >> $logFile

	for ((i=1;i<=$daysBack;i++)); 
	do
        	date=`date -u --date="$i days ago" +"%y-%m-%d"`
        	echo "Compressing logs for date: "$date >> $logFile
		filePath="$logsDirectory*$date.log"
		echo "Compressing file:"$filePath >> $logFile
		sudo -u logstash gzip -fq $filePath >> $logFile 2>> $logFile
	done
done
echo "`date '+%Y-%m-%d %H:%M:%S'` Compression finished" >> $logFile

