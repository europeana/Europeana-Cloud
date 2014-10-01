#!/bin/bash

set -e;
set -u;

timestamp="$(date +%Y%m%d_%H.%M.%s)"
host="heliopsis.man.poznan.pl"
port=8080
uisname="/uis"
mcsname="/mcs"

recordsPerProvider=2
datasetsPerProvider=2
representationNamePerCloudId=2
mimeTypeFile="application/xml"
uloadedFileName="example_metadata.xml"
mimeTypeLargeFile="application/octet-stream"
uloadedLargeFileName="example_metadata.xml" #"test_large.zip"

threads=10
rampUpPeriod=1
loops=10
connectTimeOut=200 #timeOut in miliseconds
responseTimeOut=2000 #timeOut in miliseconds
responseMCSRepresentationTimeOut=10000

interTreadQueueTimeOut=20 #timeOut in seconds
interThreadGroupTimeGap=1500
outputFilessuffix=$timestamp



jmeter -n \
-Jhost=$host \
-Jport=$port \
-Juis-name=$uisname \
-Jthreads=$threads \
-JrecordsPerProvider=$recordsPerProvider \
-JrampUpPeriod=$rampUpPeriod \
-Jloops=$loops \
-JconnectTimeOut=$connectTimeOut \
-JresponseTimeOut=$responseTimeOut \
-JinterTreadQueueTimeOut=$interTreadQueueTimeOut \
-JinterThreadGroupTimeGap=$interThreadGroupTimeGap \
-JoutputFilessuffix=$outputFilessuffix \
-t UIS_PerformanceTestCmd.jmx -l UIS_PerformanceTest_$outputFilessuffix.log


jmeter -n \
-Jhost=$host \
-Jport=$port \
-Juis-name=$uisname \
-Jmcs-name=$mcsname \
-Jthreads=$threads \
-JrampUpPeriod=$rampUpPeriod \
-JrecordsPerProvider=$recordsPerProvider \
-JdatasetsPerProvider=$datasetsPerProvider \
-JrepresentationNamePerCloudId=$representationNamePerCloudId \
-Jloops=$loops \
-JmimeTypeFile=$mimeTypeFile \
-JuloadedFileName=$uloadedFileName \
-JconnectTimeOut=$connectTimeOut \
-JresponseTimeOut=$responseTimeOut \
-JinterTreadQueueTimeOut=$interTreadQueueTimeOut \
-JinterThreadGroupTimeGap=$interThreadGroupTimeGap \
-JoutputFilessuffix=$outputFilessuffix \
-t MCS_DataSets_PerformanceTestCmd.jmx -l MCS_DataSets_PerformanceTest_$outputFilessuffix.log

jmeter -n  \
-Jhost=$host \
-Jport=$port \
-Juis-name=$uisname \
-Jmcs-name=$mcsname \
-Jthreads=$threads \
-JrampUpPeriod=$rampUpPeriod \
-JrecordsPerProvider=$recordsPerProvider \
-JrepresentationNamePerCloudId=$representationNamePerCloudId \
-Jloops=$loops \
-JmimeTypeFile=$mimeTypeFile \
-JuloadedFileName=$uloadedFileName \
-JmimeTypeLargeFile=$mimeTypeLargeFile \
-JuloadedLargeFileName=$uloadedLargeFileName \
-JconnectTimeOut=$connectTimeOut \
-JresponseTimeOut=$responseMCSRepresentationTimeOut \
-JinterTreadQueueTimeOut=$interTreadQueueTimeOut \
-JinterThreadGroupTimeGap=$interThreadGroupTimeGap \
-JoutputFilessuffix=$outputFilessuffix \
-t MCS_Representation_PerformanceTestCmd.jmx -l MCS_Representation_PerformanceTest_$outputFilessuffix.log







