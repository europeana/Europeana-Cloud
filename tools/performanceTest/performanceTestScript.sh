#!/bin/bash
# --host [hostname or IP]			Hostname or IP of testing host eg. localhost or 127.0.0.1
# --port [port number]				Port number of testing instance eg. 8080
# --loops [number of loops]			Number of request invoking by single thread.
# --threads [number of threads]			Number of threads invoking requests.
# --GUI						Gui mode works only first test.
# --testMode [JMeter test mode]			Set JMeter test mode manually eg. "-R 1.0.0.1"
# --interThreadGroupTimeGap [time in ms] 	Time gap between each thread group test single request.
# --interTestDelay [time in sec]		Delay between test cases.
# --uisPath [path to UIS]			Path to UIS service.
# --mcsPath [path to MCS]			Path to MCS service.
#
# --ganglia [path to ganglia] [ganglia period string] [ganglia node name] [ganglia cluster name]			
#    eg. --ganglia "http://localhost/ganglia" hour heliopsis apps+cluster           
#
# --Auth [username] [password]			Set basic authorization user and password.
#
# --allTests					Runs all test cases.
# --uisTest					Runs uisTest case.
# --mcsDatasetTest				Runs mcsDatasetTest case.
# --mcsRepresentationsTest			Runs mcsRepresentationsTest case.
#
#
# Eg. performanceTestScript.sh --host localhost --loops 1 --threads 1 --allTests
set -e;
set -u;

host=localhost
port=8080
loops=1 #numberOfProvider
threads=1
testMode="-n"
interThreadGroupTimeGap=1 # in miliseconds
interTestDelay=1
uisTest=false
mcsDatasetTest=false
mcsRepresentationsTest=false
user=""
password=""
uisPath="/uis"
mcsPath="/mcs"
gangliaUrl=""
gangliaPeriod="hour"
gangliaNodeName="heliopsis"
gangliaClusterName="apps+cluster"

function setAllTestsOn() { 
uisTest=true
mcsDatasetTest=true
mcsRepresentationsTest=true
}

function showHelp() {
for i in {2..21}
	do
       		echo `sed "$i!d" $0`
	done
       exit 1
}

while [ $# -gt 0 ] ; do
case $1 in
--host) shift 1 ; host=${1}; shift 1 ;;
--port) shift 1 ; port=${1}; shift 1 ;;
--loops) shift 1 ; loops=${1}; shift 1 ;;
--threads) shift 1 ; threads=${1}; shift 1 ;;
--GUI)  shift 1; testMode=""; continue ;;
--testMode) shift 1; testMode=${1};  shift 1 ;;
--interThreadGroupTimeGap) shift 1 ; interThreadGroupTimeGap=${1}; shift 1 ;;
--interTestDelay) shift 1 ; interTestDelay=${1}; shift 1 ;;
--uisPath) shift 1 ; uisPath=${1}; shift 1 ;;
--mcsPath) shift 1 ; mcsPath=${1}; shift 1 ;;
--ganglia) shift 1 ; gangliaUrl=${1} ;  shift 1 ; gangliaPeriod=${1} ; shift 1 ; gangliaNodeName=${1}  ;  shift 1 ; gangliaClusterName=${1} ; shift 1 ;;
--allTests)  shift 1 ; setAllTestsOn; continue ;;
--uisTest)  shift 1 ; uisTest=true; continue ;;
--mcsDatasetTest)  shift 1 ; mcsDatasetTest=true; continue ;;
--mcsRepresentationsTest) shift 1 ;  mcsRepresentationsTest=true; continue ;;
--Auth) shift 1; user=${1} ; shift 1 ; password=${1} ; shift 1 ;;
--help) shift 1; showHelp ;;
*) echo "Unsuppored paramiter ${1}"; shift 1 ;;
esac
done
if [ "$uisTest" = false ] &&  [ "$mcsDatasetTest" = false ]  &&  [ "$mcsRepresentationsTest" = false ]   
then 
setAllTestsOn; 
fi




#additional variables
rampUpPeriod=0
recordsPerProvider=1
datasetsPerProvider=1
representationNamePerCloudId=1

timestamp="$(date +%Y%m%d_%H.%M.%s)"

mimeTypeFile="application/xml"
uloadedFileName="example_metadata.xml"
mimeTypeLargeFile="application/octet-stream"
sizesLargefile=('1' '5' '10'); #in MiB
connectTimeOut=5000 #timeOut in miliseconds
responseTimeOut=5000 #timeOut in miliseconds
responseMCSRepresentationTimeOut=100000 #timeOut in miliseconds
interTreadQueueTimeOut=10 #timeOut in seconds
outputFilessuffix=$timestamp
folderPrefix=test_${timestamp}
cummulatedCsvFileName="cummulated_${outputFilessuffix}.csv"
uloadedLargeFileName="test.file${timestamp}" 

graphTypes=('mem_report' 'load_report' 'cpu_report' 'network_report');
gangliaDelay=$interThreadGroupTimeGap #in seconds

resultDir="./${timestamp}"


mkdir -p "${resultDir}"

echo host=$host | tee -a ${resultDir}/test.parms
echo port=$port | tee -a ${resultDir}/test.parms
echo loops=$loops | tee -a ${resultDir}/test.parms
echo threads=$threads | tee -a ${resultDir}/test.parms
echo testMode=$testMode | tee -a ${resultDir}/test.parms
echo interThreadGroupTimeGap=$interThreadGroupTimeGap | tee -a ${resultDir}/test.parms
echo interTestDelay=$interTestDelay | tee -a ${resultDir}/test.parms
echo uisTest=$uisTest | tee -a ${resultDir}/test.parms
echo mcsDatasetTest=$mcsDatasetTest | tee -a ${resultDir}/test.parms
echo mcsRepresentationsTest=$mcsRepresentationsTest | tee -a ${resultDir}/test.parms
echo gangliaUrl=$gangliaUrl | tee -a ${resultDir}/test.parms
echo user=$user | tee -a ${resultDir}/test.parms
echo password=$password | tee -a ${resultDir}/test.parms
echo uisPath=$uisPath | tee -a ${resultDir}/test.parms
echo mcsPath=$mcsPath | tee -a ${resultDir}/test.parms



#UIS test
function uisTest() {
echo "############################### UIS TEST ############################### ";
jmeter $testMode \
-Jhost=$host \
-Jport=$port \
-Juis-name=$uisPath \
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
-Juser=$user \
-Jpassword=$password \
-t ./testCases/UIS_PerformanceTestCmd.jmx -l ${resultDir}/UIS_PerformanceTest_${outputFilessuffix}.log

cat ${resultDir}/UIS_PerformanceTest_$outputFilessuffix.log >> ${resultDir}/$cummulatedCsvFileName
}

#MCS DataSet test
function mcsDataSetTest() {
echo "############################### MCS DATA_SET TEST ############################### ";
jmeter $testMode  \
-Jhost=$host \
-Jport=$port \
-Juis-name=$uisPath \
-Jmcs-name=$mcsPath \
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
-Juser=$user \
-Jpassword=$password \
-t ./testCases/MCS_DataSets_PerformanceTestCmd.jmx -l ${resultDir}/MCS_DataSets_PerformanceTest_$outputFilessuffix.log

cat ${resultDir}/MCS_DataSets_PerformanceTest_$outputFilessuffix.log >> ${resultDir}/$cummulatedCsvFileName
}

#MCS Representation test
function mcsRepresentationTest() {

for sizeLargefile in "${sizesLargefile[@]}"
do
echo "############################### MCS REPRESENTATION FOR ${sizeLargefile} MiB TESTS ############################### ";

#prepare large test file
dd if=/dev/urandom of=${resultDir}/$uloadedLargeFileName bs=512 count=$((${sizeLargefile}*1024*1024/512))

jmeter $testMode \
-Jhost=$host \
-Jport=$port \
-Juis-name=$uisPath \
-Jmcs-name=$mcsPath \
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
-Juser=$user \
-Jpassword=$password \
-t ./testCases/MCS_Representation_PerformanceTestCmd.jmx -l ${resultDir}/MCS_Representation_PerformanceTest_${sizeLargefile}_${outputFilessuffix}.log

cat ${resultDir}/MCS_Representation_PerformanceTest_${sizeLargefile}_${outputFilessuffix}.log >> ${resultDir}/$cummulatedCsvFileName
rm ${resultDir}/$uloadedLargeFileName

sleep $interTestDelay
done
}

#download graphs from ganglia
function gangliaDownload() {
echo "############################### GANGLIA GRAPH IMPORT ############################### ";
echo "<html><body>" >> ${resultDir}/${timestamp}.html
for graphType in "${graphTypes[@]}"
do
   curl $gangliaUrl\/graph.php?r=$gangliaPeriod\&z=xlarge\&h=$gangliaNodeName\&m=load_one\&s=by+name\&mc=2\&g=$graphType\&c=$gangliaClusterName > ${resultDir}/$graphType${timestamp}.png
echo " <img src="$graphType${timestamp}.png" alt="$graphType"> " >> ${resultDir}/${timestamp}.html
done
echo "</body></html>" >> ${resultDir}/${timestamp}.html
}

############################################################################
## Main program flow.
############################################################################
if [ "$uisTest" = true ] 
then 
uisTest ;
sleep $interTestDelay;
fi

if [ "$mcsDatasetTest" = true ] 
then 
mcsDataSetTest;
sleep $interTestDelay;
fi

if [ "$mcsRepresentationsTest" = true ] 
then 
mcsRepresentationTest;
sleep $interTestDelay;
fi

if [ "$gangliaUrl" != "" ] 
then 
sleep $gangliaDelay
gangliaDownload;
fi

