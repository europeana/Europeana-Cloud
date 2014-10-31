#!/bin/bash

set -e;
set -u;

timestamp="$(date +%Y%m%d_%H.%M.%s)"
host=$1
port=$2
uisname="/uis"
mcsname="/mcs"

#main setup
recordsPerProvider=1
datasetsPerProvider=1
representationNamePerCloudId=1
threads=$4
rampUpPeriod=0
loops=$3 #numberOfProvider
interThreadGroupTimeGap=$6 # in miliseconds
#inter test delay
interTestDelay=$7
mimeTypeFile="application/xml"
uloadedFileName="example_metadata.xml"
mimeTypeLargeFile="application/octet-stream"
sizesLargefile=('1' '5' '10'); #in MiB
connectTimeOut=5000 #timeOut in miliseconds
responseTimeOut=5000 #timeOut in miliseconds
responseMCSRepresentationTimeOut=100000 #timeOut in miliseconds
interTreadQueueTimeOut=10 #timeOut in seconds
outputFilessuffix=$timestamp
#gagnglia statistic download configuration
gangliaUrl="http://ismelia.man.poznan.pl/ganglia"
gangliaPeriod="hour"
gangliaNodeName="heliopsis"
gangliaClusterName="apps+cluster"
graphTypes=('mem_report' 'load_report' 'cpu_report' 'network_report');
gangliaDelay=$interThreadGroupTimeGap #in seconds

folderPrefix=test_${timestamp}
cummulatedCsvFileName="cummulated_${outputFilessuffix}.csv"
uloadedLargeFileName="test.file${timestamp}" 
resultDir="./${timestamp}"
testMode=$5 

mkdir -p "${resultDir}"





#UIS test
function uisTest {
jmeter $testMode \
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
-JoutputFilespreffix=${resultDir}/ \
-t ./testCases/UIS_PerformanceTestCmd.jmx -l ${resultDir}/UIS_PerformanceTest_${outputFilessuffix}.log

cat ${resultDir}/UIS_PerformanceTest_$outputFilessuffix.log >> ${resultDir}/$cummulatedCsvFileName
}

#MCS DataSet test
function mcsDataSetTest {
jmeter $testMode  \
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
-JoutputFilespreffix=${resultDir}/ \
-t ./testCases/MCS_DataSets_PerformanceTestCmd.jmx -l ${resultDir}/MCS_DataSets_PerformanceTest_$outputFilessuffix.log

cat ${resultDir}/MCS_DataSets_PerformanceTest_$outputFilessuffix.log >> ${resultDir}/$cummulatedCsvFileName
}

#MCS Representation test
function mcsRepresentationTest {
for sizeLargefile in "${sizesLargefile[@]}"
do

echo "MCS_Representation_PerformanceTest for ${sizeLargefile} MiB"
#prepare large test file
dd if=/dev/urandom of=${resultDir}/$uloadedLargeFileName bs=512 count=$((${sizeLargefile}*1024*1024/512))

jmeter $testMode \
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
-JuloadedLargeFileName=../${resultDir}/$uloadedLargeFileName \
-JconnectTimeOut=$connectTimeOut \
-JresponseTimeOut=$responseMCSRepresentationTimeOut \
-JinterTreadQueueTimeOut=$interTreadQueueTimeOut \
-JinterThreadGroupTimeGap=$interThreadGroupTimeGap \
-JoutputFilessuffix=${sizeLargefile}_$outputFilessuffix \
-JlargeFileSize=$sizeLargefile \
-JoutputFilespreffix=${resultDir}/ \
-t ./testCases/MCS_Representation_PerformanceTestCmd.jmx -l ${resultDir}/MCS_Representation_PerformanceTest_${sizeLargefile}_${outputFilessuffix}.log

cat ${resultDir}/MCS_Representation_PerformanceTest_${sizeLargefile}_${outputFilessuffix}.log >> ${resultDir}/$cummulatedCsvFileName
rm ${resultDir}/$uloadedLargeFileName

sleep $interTestDelay
done
}

#download graphs from ganglia
function gangliaDownload {
echo "<html><body>" >> ${resultDir}/${timestamp}.html
for graphType in "${graphTypes[@]}"
do
   curl $gangliaUrl\/graph.php?r=$gangliaPeriod\&z=xlarge\&h=$gangliaNodeName\&m=load_one\&s=by+name\&mc=2\&g=$graphType\&c=$gangliaClusterName > ${resultDir}/$graphType${timestamp}.png
echo " <img src="$graphType${timestamp}.png" alt="$graphType"> " >> ${resultDir}/${timestamp}.html
done
echo "</body></html>" >> ${resultDir}/${timestamp}.html
#mkdir $folderPrefix
#mv *${timestamp}* ./${folderPrefix}/
}

echo $@ > ${resultDir}/test.parms
uisTest
sleep $interTestDelay
mcsDataSetTest
sleep $interTestDelay
mcsRepresentationTest
sleep $gangliaDelay
gangliaDownload


