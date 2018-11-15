#!/bin/bash

#send random messages via Kafka console producer, formatted as Solr-accepted log messages
#2014-07-15T14:10:20.123Z mcs mcs01 - ERROR: We can't communicate.

###script configuration
set -e
set -u
shopt -s nullglob

if [ $# -ne 3 ]
then
	echo usage: "kafkaLogFortune.sh <producerCall> <sleepInterval> <timeSpectrum>"
	exit -1
fi

###parameters
producerCall="$1"
sleepInterval=$2 #in seconds, can be fractional
timeSpectrum=$3 #from how many minutes from before to draw from

###values to draw from
priorityArray=("FATAL" "ERROR" "WARN" "INFO" "DEBUG" "TRACE")
serviceTypeArray=("mcs" "uis")
serviceIdArray=("01" "02" "03" "04")

###functions
#this works because it treats all passed parameters as an array
function drawFromArray
{
	local -a array=("${@}")	
	local num=${#array[@]}
	local num=$((num-1))
	local random=$(shuf -i0-$num -n1)
	echo "${array[$random]}"
}

function drawMessage
{
	echo `fortune`
}

function drawTimestamp
{
	local minutesAgo=$(shuf -i0-$timeSpectrum -n1)
	echo `date -u --date="$minutesAgo minutes ago" +"%Y-%m-%dT%H:%M:%S.%3NZ"`
}

###program
echo "Entering infinite loop. Turn off with Ctrl+C."

while true
do
	timestamp=$(drawTimestamp)
	serviceType=$(drawFromArray "${serviceTypeArray[@]}")
	serviceId=$(drawFromArray "${serviceIdArray[@]}")
	priority=$(drawFromArray "${priorityArray[@]}")
	message=$(drawMessage)
	echo "$timestamp $serviceType $serviceType$serviceId - ${priority}: $message" | $producerCall
	sleep $sleepInterval
done


