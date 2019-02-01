#!/bin/bash
#
# This script will:
#   1. Flush data from all specified keyspaces;
#   2. Create snapshot
#   3. Run 'fullBackupCreator.sh' for each keyspace and created snapshot
#
# This scrupt is executed by cron na each cassandra machine.
#

#exit script of first failed command
set -e

keyspacesToBeBackuped=(
    ecloud_mcs_v12)

backupTimeInDays=`date +"%Y-%m-%d"`
dataLocation=~/nosql_filesystem/cassandra/data/data/
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo '' |  mailx -s "[`hostname -s`] About to start fullBackup" mail@to
echo -e "${GREEN}Flushing and creating snapshot with name $backupTimeInDays${NC}"
for keyspaceToBeBackuped in "${keyspacesToBeBackuped[@]}"
do
    echo -e "${GREEN}\tFlushing $keyspaceToBeBackuped${NC}"
    nodetool flush $i
    echo -e "${GREEN}\tCreating snapshots for: $keyspaceToBeBackuped${NC}"
    snapshotDirName="backup_$backupTimeInDays"
    nodetool snapshot -t $snapshotDirName $keyspaceToBeBackuped
    echo -e "${GREEN}Removing incremental backups${NC}"
    for table in $dataLocation$keyspaceToBeBackuped/*
    do
        tableBackupsDir=$table/backups/
        echo -e "${GREEN}\tRemoving backups directory content for $tableBackupsDir${NC}"

        if [ ! -d "$tableBackupsDir" ]; then
            echo -e "${GREEN}\t\t[WARN]Directory '$tableBackupsDir' does not exist. Skipping.${NC}"
            continue
        fi
        if [ ! "$(ls -A $tableBackupsDir)" ]; then
            echo -e "${GREEN}\t\t[WARN]Directory '$tableBackupsDir' is empty. Skipping.${NC}"
            continue
        fi

        for backupFile in $tableBackupsDir*
        do
            echo "Removing $backupFile"
            rm $backupFile
        done

    done
done

for keyspaceToBeBackuped in "${keyspacesToBeBackuped[@]}"
do
    echo -e "${GREEN}Executing full backup script for $keyspaceToBeBackuped and $snapshotDirName${NC}"
    ./fullBackupCreator.sh -s $snapshotDirName
done
echo '' |  mailx -s "[`hostname -s`] Full backup finished" mail@to
