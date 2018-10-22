#!/bin/bash
#
# This script create database backup:

#exit script of first failed command
#set -e

keyspacesToBeBackuped=(
    production_ecloud_mcs)

backupTime=`date +"%Y-%m-%d"`
dataLocation=~/playground/sampleCass/

################
##
################

echo "Preparing backup"

echo "Flushing dbs"

for i in "${keyspacesToBeBackuped[@]}"
    do
        echo -e "\tFlushing: $i"
    done

echo "Creating snapshots"

snapshotDirName="backup_$backupTime"

for i in "${keyspacesToBeBackuped[@]}"
    do
        if [ -d "$snapshotDirName" ]; then
            echo "Directory was already created. Script will be stopped."
            exit
        fi
        echo -e "\tCreating snapshots for: $i in '$snapshotDirName' directory."
    done

echo "About to start backups generation"

for i in "${keyspacesToBeBackuped[@]}"
        do
            echo -e "\tWill backup: $i"

            for keyspace in $dataLocation$i/* ; do
                for table in $keyspace ; do
                    echo -e "\t\tGenerating tarball for $table"
                    tar --exclude=sample.tar -cvf $table/snapshot/$snapshotDirName/sample.tar $table/snapshot/$snapshotDirName/*

                    echo -e "\t\tCopying created tarball to backup directory"
                    #rsync
                    echo -e "\t\tRemoving created tarball from local directory"
                    #rm
                done
            done
        done