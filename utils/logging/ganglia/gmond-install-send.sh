#!/bin/bash

#what is script for?
#install Gmond on server using

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
shopt -s nullglob

#check number of parameters
if [ $# -lt 8 ]
then
	echo "usage: sendGmond <sourceDir> <tempDir> <sshAddress> <clusterName> <user> <hostnameShort> <receive> <aggregators...>"
	exit -1
fi

sourceDir=$1
shift
tempDir=$1
shift
sshAddress=$1
shift
clusterName=$1
shift
user=$1
shift
hostnameShort=$1
shift
receive=$1
shift
aggregators=($@)

#this are also parameters
#but we don't want to change them from installation to installation
sendIterval=60
gridName=ecloud


##prepare files for sending - fill parameters in installation files and target host script
#create temporary folder for configuring files
rm -rf $tempDir
mkdir -p $tempDir/install
cp -r $sourceDir/* $tempDir/install
#configure gmond.conf
sed -i "s/user\ =\ nobody/user\ =\ $user/g" $tempDir/install/etc/gmond.conf
sed -i "s/send_metadata_interval\ =\ 0/send_metadata_interval\ =\ 60/g" $tempDir/install/etc/gmond.conf
#remove udp_send_channel, mark where it was
perl -0777 -pi -e "s/\nudp_send_channel\ {\n([^}]*)}/\nXXX/g" $tempDir/install/etc/gmond.conf


