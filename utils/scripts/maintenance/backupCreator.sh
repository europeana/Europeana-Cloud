#!/bin/bash
#
# This script create database backup:

#exit script of first failed command
#set -e

keyspacesToBeBackuped=(
    ecloud_mcs_v12)

backupTime=`date +"%Y-%m-%d"`
dataLocation=~/nosql_filesystem/cassandra/data/data/
backupLocation=~/mnt/ARCHIVE/test
machine=`hostname`
################
##
################

check_request_valid () {
    echo "Checking request"
    keyspacesExists

}

keyspacesExists () {
    for i in "${keyspacesToBeBackuped[@]}"
        do
            if [ ! -d "$dataLocation$i" ]; then
                echo -e "\t'$i' does not exist. Stoping execution."
                exit
            fi
        done
    echo -e "\tKeyspaces exists"
}

################
##
################

check_request_valid

echo "Preparing backup"

while getopts ":s:" opt; do
case $opt in
    s)
      snapshotToBeUsed=$OPTARG
      echo "Snapshot name was provided. We will use '$snapshotToBeUsed' for backup."
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
 esac
done

if [ -z ${snapshotToBeUsed+x} ];
    then
        echo "Snapshot name is not specified. New snapshot will be created.";
        echo "Flushing dbs"

        for i in "${keyspacesToBeBackuped[@]}"
            do
                echo -e "\tFlushing: $i"
		        nodetool flush $i
            done

        echo "Creating snapshots"

        snapshotDirName="backup_$backupTime"
        snapshotToBeUsed=$snapshotDirName

        for i in "${keyspacesToBeBackuped[@]}"
            do
                echo -e "\tCreating snapshots for: $i in '$snapshotDirName' directory."
                nodetool snapshot -t $snapshotDirName $i
            done
    else
        echo "Snapshot '$snapshotToBeUsed' will be used."; fi

echo "About to start backups generation"

for i in "${keyspacesToBeBackuped[@]}"
        do
            echo -e "\tWill backup: $i"
            for table in $dataLocation$i/*
            do
                echo -e "\t\tGenerating tarball for ${table}"

            if [ ! -d "$table/snapshots/$snapshotToBeUsed/" ]; then
                    echo -e "\t\t[WARN]Directory '$table/snapshots/$snapshotToBeUsed/' does not exist. Skipping."
                    continue
                fi
		cd $table/snapshots/$snapshotToBeUsed/
                tar --warning=none --exclude=${table##*/}.tar -cf ${table##*/}.tar *

		backupPath=$backupLocation/$backupTime/$i/${table##*/}/$machine
                echo -e "\t\tCopying created tarball ${table##*/}.tar to backup directory $backupPath"
		mkdir -p $backupPath
		rsync --progress -vh ${table##*/}.tar $backupPath
                echo -e "\t\tRemoving created tarball from local directory"
                rm ${table##*/}.tar
                echo -e " "
            done
        done