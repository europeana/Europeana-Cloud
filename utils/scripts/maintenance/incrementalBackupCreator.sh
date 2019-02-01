#!/bin/bash
#
# This script will create incremental database backup in archive location.
# For each table in each keyspace it will:
#   1. Tar all the files from /backup directory
#   2. Copy tarball to archive (corylus)
#   3. Remove created tarball
#   4. Remove all files that was archived from local machine
#
# Generated file will have current date in its name.

#exit script of first failed command
set -e

keyspacesToBeBackuped=(
    production_ecloud_mcs_v12)

backupTimeInDays=`date +"%Y-%m-%d"`
backupTimeInMonths=`date +"%Y-%m"`
dataLocation=~/nosql_filesystem/cassandra/data/data/
backupLocation=/mnt/backup
GREEN='\033[0;32m'
NC='\033[0m' # No Color
###################
#
###################

echo -e "${GREEN}Preparing incremental backup${NC}"
for keyspaceToBeBackuped in "${keyspacesToBeBackuped[@]}"
do
    echo -e "${GREEN}Will backup keyspace: $keyspaceToBeBackuped${NC}"
    for table in $dataLocation$keyspaceToBeBackuped/*
    do
        echo -e "${GREEN}Will backup table: ${table##*/}${NC}"
        backupDirLocation=$table/backups
        echo -e "${GREEN}\tGenerating tarball for $backupDirLocation${NC}"
        if [ ! -d "$table/backups/" ]; then
                echo -e "${GREEN}\t[WARN]Directory '$backupDirLocation' does not exist. Skipping.${NC}"
                continue
        fi
        if [ ! "$(ls -A $table/backups/)" ]; then
        echo -e "${GREEN}\t[WARN]Directory '$backupDirLocation' is empty. Skipping.${NC}"
            continue
        fi
        cd $backupDirLocation
        filesToBeArchived=( $( ls . ) )
        backupFileName=${backupTimeInDays}_backup.tar
        tar --warning=none --exclude=${backupFileName} -cvf ${backupFileName} ${filesToBeArchived[@]}
        backupPath=$backupLocation/${backupTimeInMonths}/$keyspaceToBeBackuped/${table##*/}/backups
        echo -e "${GREEN}\tCopying created tarball $backupFileName to backup directory $backupPath${NC}"
        mkdir -p $backupPath
        rsync --progress -vh $backupFileName $backupPath
        echo -e "${GREEN}\tRemoving created tarball from local directory${NC}"
        rm $backupFileName
        echo -e "${GREEN}\tRemoving backup files from local directory: ${filesToBeArchived[@]} ${NC}"
        rm ${filesToBeArchived[@]}
    done
done
