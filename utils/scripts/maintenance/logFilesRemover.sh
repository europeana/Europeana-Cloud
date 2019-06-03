#!/bin/bash
#
# Will remove all files matching regex '*{date}.log.gz' in 'logsDirectories' directories.
# For now date is set to five days back.
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
	/hoem/centos/eCloudLogs/aas/ )
daysBack=${1:-5}

echo "`date '+%Y-%m-%d %H:%M:%S'` Files removal started" >> $logFile
echo "Days back: "$daysBack >> $logFile

touch $logFile
for logsDirectory in "${logsDirectories[@]}"
do
	echo "Removing log files for directory: "$logsDirectory >> $logFile

	date=`date -u --date="$daysBack days ago" +"%y-%m-%d"`
	echo "Removing logs for date: "$date >> $logFile
	filePath="$logsDirectory*$date.log.gz"
	echo "Removing file:"$filePath >> $logFile
	sudo -u logstash rm $filePath 2>>$logFile
done
echo "`date '+%Y-%m-%d %H:%M:%S'` Removal finished" >> $logFile

