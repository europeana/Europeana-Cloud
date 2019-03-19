#!/bin/bash
#
# This script will create database backup in archive location.
#
# It can be run in two modes.
# First one mode will do the flush and snapshot on selected keyspaces. All the files stored in snapshot directory
# will be combined to one file (tar) and copied to archive. To use this mode we just have to execute this script
# without parameters:
#
# ./backupCreator.sh
#
# Second one mode will do the backup for the specified snapshot (no flush and no new snapshot creation will be executed).
# Following snippet shows how to execute script in this mode:
#
# ./backupCreator.sh -s snapshotName
#
#

#exit script of first failed command
set -e

keyspacesToBeBackuped=(
    production_ecloud_mcs_v12)

backupTimeInDays=`date +"%Y-%m-%d"`
backupTimeInMonths=`date +"%Y-%m"`
dataLocation=~/nosql_filesystem/cassandra/data/data/
backupLocation=/mnt/backup
machine=`hostname`
GREEN='\033[0;32m'
NC='\033[0m' # No Color
################
##
################

check_request_valid () {
    echo -e "${GREEN}Checking request${NC}"
    keyspacesExists

}

keyspacesExists () {
    for i in "${keyspacesToBeBackuped[@]}"
        do
            if [ ! -d "$dataLocation$i" ]; then
                echo -e "${GREEN}\t'$i' does not exist. Stoping execution.${NC}"
                exit
            fi
        done
    echo -e "${GREEN}\tKeyspaces exists${NC}"
}

################
##
################

check_request_valid

echo  -e "${GREEN}Preparing backup${NC}"

while getopts ":s:" opt; do
case $opt in
    s)
      snapshotToBeUsed=$OPTARG
      echo -e "${GREEN}Snapshot name was provided. We will use '$snapshotToBeUsed' for backup.${NC}"
      ;;
    \?)
      echo -e "${GREEN}Invalid option: -$OPTARG${NC}" >&2
      exit 1
      ;;
    :)
      echo -e "${GREEN}Option -$OPTARG requires an argument.${NC}" >&2
      exit 1
      ;;
 esac
done

if [ -z ${snapshotToBeUsed+x} ];
    then
        echo -e "${GREEN}Snapshot name is not specified. New snapshot will be created.${NC}";
        echo -e "${GREEN}Flushing dbs${NC}"

        for i in "${keyspacesToBeBackuped[@]}"
            do
                echo -e "${GREEN}\tFlushing: $i${NC}"
		        nodetool flush $i
            done

        echo -e "${GREEN}Creating snapshots${NC}"

        snapshotDirName="backup_$backupTimeInDays"
        snapshotToBeUsed=$snapshotDirName

        for i in "${keyspacesToBeBackuped[@]}"
            do
                echo -e "${GREEN}\tCreating snapshots for: $i in '$snapshotDirName' directory.${NC}"
                nodetool snapshot -t $snapshotDirName $i
            done
    else
        echo -e "${GREEN}Snapshot '$snapshotToBeUsed' will be used.${NC}"; fi

echo -e "${GREEN}About to start backups generation${NC}"

for i in "${keyspacesToBeBackuped[@]}"
do
    echo -e "${GREEN}\tWill backup: $i${NC}"
    for table in $dataLocation$i/*
    do
        echo -e "${GREEN}\t\tGenerating tarball for ${table}${NC}"

        if [ ! -d "$table/snapshots/$snapshotToBeUsed/" ]; then
            echo -e "${GREEN}\t\t[WARN]Directory '$table/snapshots/$snapshotToBeUsed/' does not exist. Skipping.${NC}"
            continue
        fi
        cd $table/snapshots/$snapshotToBeUsed/
        tar --warning=none --exclude=${table##*/}.tar -cvf ${table##*/}.tar *

        backupPath=$backupLocation/$backupTimeInMonths/$i/${table##*/}
        echo -e "${GREEN}\t\tCopying created tarball ${table##*/}.tar to backup directory $backupPath${NC}"
        mkdir -p $backupPath
        rsync --progress -vh ${table##*/}.tar $backupPath
        echo -e "${GREEN}\t\tRemoving created tarball from local directory${NC}"
        rm ${table##*/}.tar
        echo -e " "
    done
done