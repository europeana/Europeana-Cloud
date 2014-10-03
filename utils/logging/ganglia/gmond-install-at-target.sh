#!/bin/bash

#what is script for?
#install Gmond on server using

#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u
shopt -s nullglob

#check number of parameters
if [ $# -ne 4 ]
then
	echo "usage: sendGmond <sourceDir> <sshAddress> <clusterName> <hostnameShort> <aggregators>"
	exit -1
fi

sourceDir=$1
shift
sshAddress=$1
shift
clusterName=$1
shift
hostnameShort=$1
shift


ssh $sshAddress "mkdir -p tools install"
scp -r $source/gmond-run.sh $sshAddress:tools
scp -r $source/install-$clusterName.tar.gz $sshAddress:install.tar.gz
ssh $sshAddress "tar -xzf install.tar.gz" 
ssh $sshAddress "sed -i \"s/CLUSTER cluster/$clusterName cluster/g\" install/etc/gmond.conf"
ssh $sshAddress "sed -i \"s/HOSTNAME/$hostnameShort/g\" install/etc/gmond.conf"
ssh $sshAddress 'echo export PATH="\$HOME/install/bin:\$PATH" >> .bashrc'
ssh $sshAddress 'echo export PATH="\$HOME/install/sbin:\$PATH" >> .bashrc'

