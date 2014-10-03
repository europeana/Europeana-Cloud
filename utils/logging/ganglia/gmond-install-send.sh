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
owner=ecloud


###prepare files for sending - fill parameters in installation files and target host script
##create temporary folder for configuring files
rm -rf $tempDir
mkdir -p $tempDir/install
cp -r $sourceDir/* $tempDir/install

##configure gmond.conf
sed -i "s/user\ =\ nobody/user\ =\ $user/g" $tempDir/install/etc/gmond.conf
sed -i "s/send_metadata_interval\ =\ 0/send_metadata_interval\ =\ 60/g" $tempDir/install/etc/gmond.conf
sed -i "s/# override_hostname\ =\ \"mywebserver.domain.com\"/override_hostname\ =\ \"$hostnameShort\"/g" $tempDir/install/etc/gmond.conf
sed -i "s/name\ = \"unspecified\"/name\ =\ \"$clusterName cluster\"/g" $tempDir/install/etc/gmond.conf
sed -i "s/owner\ = \"unspecified\"/owner\ =\ \"$owner\"/g" $tempDir/install/etc/gmond.conf

#remove udp_send_channel, mark where it was
perl -0777 -pi -e "s/\nudp_send_channel\ {\n([^}]*)}/\nXXX/g" $tempDir/install/etc/gmond.conf
#for each agregators set one udp_send_channel:
#first, prepare text
channelText=""
for ((i=0; i < ${#aggregators[@]}; i++))
do
	channelText="$channelText\nudp_send_channel {"
	channelText="$channelText\n  host = ${aggregators[$i]}"
	channelText="$channelText\n  port = 8649"
	channelText="$channelText\n  ttl = 1"
	channelText="$channelText\n}"
done
#then replace XXX with it
perl -0777 -pi -e "s/\nXXX/$channelText/g" $tempDir/install/etc/gmond.conf

#remove some setting from udp_recv
perl -0777 -pi -e "s/\n\ \ bind\ =\ 239.2.11.71//g" $tempDir/install/etc/gmond.conf
#if we don't want to receive, remove udp_recv entirely
if [ $receive -ne 1 ]
then
	perl -0777 -pi -e "s/\nudp_recv_channel\ {\n([^}]*)}/\n/g" $tempDir/install/etc/gmond.conf
fi

#add script for running gmond to files for sending
mkdir -p $tempDir/tools
echo 'INSTALL_ROOT=$HOME/install' >> $tempDir/tools/gmond-run.sh
echo '' >> $tempDir/tools/gmond-run.sh
echo 'gmond -c $INSTALL_ROOT/etc/gmond.conf' >> $tempDir/tools/gmond-run.sh
chmod u+x $tempDir/tools/gmond-run.sh

#copy everything to the remote server
scp -r $tempDir/* $sshAddress:
#run gmond deamon
ssh $sshAddress tools/gmond-run.sh
