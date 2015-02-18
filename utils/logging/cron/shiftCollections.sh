#!/bin/bash

#create new per-UTC-day core in Solr
#create alias "default" for it

#we just entered day 15-07-2014
#we will create core 'core-2014-07-15'
#if $1==0 we will immediately remove core 'core-2014-07-14'
#if $1==1 we will immediately remove core 'core-2014-07-13'

#if $1==7 we will immediately remove core 'core-2014-07-07',
#which will leave history of the last week

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
#wildcard * 
shopt -s nullglob


if [ $# -ne 8 ]
then
	echo usage: "shiftCollections.sh <num-of-days-to-preserve> <solrHTTP> <dataDirRoot> <configSrcDir> <configTempDir> <zkHome> <zkHost> <solrLibDir>"
	exit -1
fi



numPreserve=$1
numDel=$((numPreserve + 1)) 
solrHTTP=$2
dataDirRoot=$3
configSrcDir=$4
configTempDir=$5
zkHome=$6
zkHost=$7
solrLibDir=$8

curAlias="core-today"
globalAlias="core-all-alias"

#obtain current UTC date
curDate=`date -u +"%Y-%m-%d"`
#this is name for collection and for core
curName="core-$curDate"

#create dataDir for new core
curDataDir="$dataDirRoot/$curName"
#mkdir -p $curDataDir

#create configTempDir for config files
mkdir -p $configTempDir
#copy config files to configTempDir  
cp $configSrcDir/schema.xml $configTempDir
cp $configSrcDir/solrconfig.xml $configTempDir
#set ${solr.data.dir} and ${solr.ulog.dir:} to dataDir for new core
sed -i "s|<dataDir>\${solr.data.dir}</dataDir>|<dataDir>$curDataDir</dataDir>|g" $configTempDir/solrconfig.xml
#sed -i "s|<dataDir>\${solr.data.dir}</dataDir>|<dataDir>$curDataDir</dataDir>|g" $configTempDir/solrconfig.xml

#upload this configuration to zookeeper
#normal zkCli.sh from zookeeper installation 
#might work as well, but it needs solr jars anyway
classPath='.'
classPath="${classPath}:${solrLibDir}/*"
classPath="${classPath}:${zkHome}/lib/*"
zkClass='org.apache.solr.cloud.ZkCLI'
#if we upload configuration with name of collection, it will be default for creation of this collection
java -cp "${classPath}" $zkClass -cmd upconfig -zkhost $zkHost -confdir $configTempDir -confname $curName

#create new core
#collection with the same name will be automatically created
newCoreCall="$solrHTTP/admin/cores?action=CREATE&name=$curName&replicationFactor=1&numShards=1"
curl "$newCoreCall" -H -d

#change alias from previous-day-core to current-day-core
#if alias exists it will be replaced - like atomic 'move' command (luckily!)
moveAliasCall="$solrHTTP/admin/collections?action=CREATEALIAS&name=$curAlias&collections=$curName"
curl "$moveAliasCall" -H -d

#obtain the list of existing collections
selectColCall="$solrHTTP/admin/cores?action=STATUS"

#if call to xmllint is the last one in pipe chain
#script will end if xmllint errors
existingColsOutput=`curl -s "$selectColCall" -H -d | /home/ecloud/install/bin/xmllint --xpath "/response/lst[@name='status']/lst/@name" -`
existingCols=(`echo $existingColsOutput | sed -e 's|name="\([^"]\+\)"|\1\n|g'`)

#DEBUG
#for ((i=0; i < ${#existingCols[@]}; i++))
#do
#	echo ${existingCols[$i]}
#done

#obtain the list of allowed collections
#include alias and current day
#also create a list of collections that should be covered by global alias
#we don't have to add alias to allowed, because they are not included when listing collections
allowedCols+=($curName)
for (( i=1; i<=$numPreserve; i++ ))
do
	dateAgo=`date -u --date="$i days ago" +"%Y-%m-%d"`
	allowedName="core-$dateAgo"
	allowedCols+=("$allowedName")	
done

#initiate the list of nodes that should be covered by global alias
globalAliasCols="$curName"

#iterate existingCols
#if it is not in allowedCols, delete
for ((i=0; i < ${#existingCols[@]}; i++))
do
	#if this is curName, do nothing
	#it is already in globaAliasCols
	#and we don't have to check it this is allowed
	if [ ${existingCols[$i]} == $curName ]
	then
		continue
	fi	

	#check if in allowedCols	
	inAllowed=false
	for ((j=0; j < ${#allowedCols[@]}; j++))
	do
		if [ ${existingCols[$i]} == ${allowedCols[$j]} ]
		then
			inAllowed=true
			break
		fi
	done

	#if not in allowedCols then delete core
	if [ "$inAllowed" = false ]
	then
		delColCall="$solrHTTP/admin/collections?action=DELETE&name=${existingCols[$i]}"
		curl "$delColCall" -H -d 
		#remove this collection config
		java -cp "${classPath}" $zkClass -cmd clear "/configs/${existingCols[$i]}" -zkhost $zkHost 
	else
		#if allowed then 
		#add to list of collections that should be covered by global alias
		globalAliasCols="$globalAliasCols,${existingCols[$i]}" 
	fi

done

#create alias for all allowed collections
globalAliasCall="$solrHTTP/admin/collections?action=CREATEALIAS&name=$globalAlias&collections=$globalAliasCols"
curl "$globalAliasCall" -H -d


#clean up
rm -r $configTempDir
