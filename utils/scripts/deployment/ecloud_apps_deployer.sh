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
newAppDirectory=~/eCloud_apps/to_be_deployed/
applicationsToBeDeployed=( aas uis mcs services)

echo "Copying old apps to backup directory"
backupTime=`date +"%Y-%m-%d-%H:%M:%S"`
mkdir $backupDirectory$backupTime

function removeApplications {
    for i in "${applicationsToBeDeployed[@]}"
    do
            echo -e "\tRemoving: "$1$i
            rm -R $1$i/*
    done
}

for i in "${applicationsToBeDeployed[@]}"
do
        echo -e "\tCopying to: "$backupDirectory$backupTime/$i
        cp -R $webAppsDirectory$i/ $backupDirectory$backupTime/$i/

done

echo "Copying new apps to backup directory"
mkdir $newAppDirectory$backupTime

for i in "${applicationsToBeDeployed[@]}"
do
        echo -e "\tCopying to: "$newAppDirectory$backupTime/$i
        cp -R $newAppDirectory$i/ $newAppDirectory$backupTime/$i/
done

echo "Removing apps from tomcat server"
removeApplications $webAppsDirectory

echo "Copying new apps to tomcat server"
for i in "${applicationsToBeDeployed[@]}"
do
        echo -e "\tCopying: "$i" to "$webAppsDirectory
        cp -R $newAppDirectory$i $webAppsDirectory
done

echo "Removing new apps from deployment directory"
removeApplications $newAppDirectory
