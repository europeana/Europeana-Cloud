#!/bin/bash
#
# This script will:
# 1. Copy working applications from tomcat 'webapps' directory to backup directory;
# 2. Remove applications from 'webapps' directory;
# 3. Copy new applications to 'webapps' directory;

#exit script of first failed command
#set -e

webAppsDirectory=~/tomcat/webapps/
backupDirectory=~/eCloud_apps/backup/
newAppDirectory=~/eCloud_apps/to_be_deployed/20170616/
applicationsToBeDeployed=( aas uis mcs dps)

echo "Copying apps to backup directory"
backupTime=`date +"%Y-%m-%d-%H:%M:%S"`
mkdir $backupDirectory$backupTime

for i in "${applicationsToBeDeployed[@]}"
do
        echo -e "\tCopying to: "$backupDirectory$backupTime/$i
        cp -R $webAppsDirectory$i/ $backupDirectory$backupTime/$i/

done
echo "Removing apps from tomcat server"
for i in "${applicationsToBeDeployed[@]}"
do
        echo -e "\tRemoving: "$webAppsDirectory$i
        rm -R $webAppsDirectory$i
done

echo "Copying new apps to tomcat server"
for i in "${applicationsToBeDeployed[@]}"
do
        echo -e "\tCopying: "$i
        mkdir $webAppsDirectory/$i/
        cp -R $newAppDirectory$i $webAppsDirectory
done


