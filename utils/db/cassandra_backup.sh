#!/bin/bash
#
# Author: Sharad Kumar Chhetri
# Date : 27-April-2015
# Description : The backup script will complete the backup in 2 phases -
#  1. First Phase: Taking backup of Keyspace SCHEMA
#  2. Seconf Phase: Taking snapshot of keyspaces
#

## In below given variables - require information to be feed by system admin##
# For _NODETOOL , you can replace $(which nodetool) with  absolute path of nodetool command.
#

#USAGE:
#./cassandra_backup.sh <NODE_IP> <KEYSPACE_NAME> <USERNAME> '<PASSWORD>'
#


#exit script of first failed command
set -e
#forbid using unset variables in bash script
set -u

_BACKUP_DIR=/home/synat/eCloud_backup
_DATA_DIR=/home/synat/nosql_filesystem/cassandra/data/data
_NODETOOL=$(which nodetool)

_NODE_IP=$1
_SCHEMA_TO_BACKUP=$2
_USER_NAME=$3
_USER_PASSWORD=$4

## Do not edit below given variable ##

_TODAY_DATE=$(date +%F)
_BACKUP_SNAPSHOT_DIR="$_BACKUP_DIR/$_TODAY_DATE/SNAPSHOTS"
_BACKUP_SCHEMA_DIR="$_BACKUP_DIR/$_TODAY_DATE/SCHEMA"
_SNAPSHOT_DIR=$(find $_DATA_DIR -type d -name snapshots)
_SNAPSHOT_NAME=snp-$(date +%F-%H%M-%S)
_DATE_SCHEMA=$(date +%F-%H%M-%S)

###### Create / check backup Directory ####

if [ -d  "$_BACKUP_SCHEMA_DIR" ]
then
echo "$_BACKUP_SCHEMA_DIR already exist"
else
mkdir -p "$_BACKUP_SCHEMA_DIR"
fi

if [ -d  "$_BACKUP_SNAPSHOT_DIR" ]
then
echo "$_BACKUP_SNAPSHOT_DIR already exist"
else
mkdir -p "$_BACKUP_SNAPSHOT_DIR"
fi

##################### SECTION 1 : SCHEMA BACKUP ############################################

echo "Creating schema backup"
if [ -d $_BACKUP_SCHEMA_DIR/$_SCHEMA_TO_BACKUP ]
then
echo "$_BACKUP_SCHEMA_DIR/$_SCHEMA_TO_BACKUP  directory exist"
else
mkdir -p $_BACKUP_SCHEMA_DIR/$_SCHEMA_TO_BACKUP
fi

cqlsh $_NODE_IP -u $_USER_NAME -p $_USER_PASSWORD -e "DESC KEYSPACE  $_SCHEMA_TO_BACKUP" > "$_BACKUP_SCHEMA_DIR/$_SCHEMA_TO_BACKUP/$_SCHEMA_TO_BACKUP"_schema-"$_DATE_SCHEMA".cql

echo "Schema backup created"

##################### END OF LINE ---- SECTION 1 : SCHEMA BACKUP #####################

###### Create snapshots for all keyspaces

echo "Creating snapshots for keyspace ....."
$_NODETOOL snapshot -t $_SNAPSHOT_NAME $_SCHEMA_TO_BACKUP

_SNAPSHOT_DIR_LIST=`find $_DATA_DIR/$_SCHEMA_TO_BACKUP -type d -name snapshots|awk '{gsub("'$_DATA_DIR/$_SCHEMA_TO_BACKUP'", "");print}' > snapshot_dir_list`

#echo $_SNAPSHOT_DIR_LIST > snapshot_dir_list

## Create directory inside backup directory. As per keyspace name.
for i in `cat snapshot_dir_list`
do
if [ -d $_BACKUP_SNAPSHOT_DIR/$i ]
then
echo "$i directory exist"
else
mkdir -p $_BACKUP_SNAPSHOT_DIR/$i
echo $i Directory is created
fi
done

### Copy default Snapshot dir to backup dir

find $_DATA_DIR -type d -name $_SNAPSHOT_NAME > snp_dir_list

for SNP_VAR in `cat snp_dir_list`;
do
## Triming _DATA_DIR
_SNP_PATH_TRIM=`echo $SNP_VAR|awk '{gsub("'$_DATA_DIR/$_SCHEMA_TO_BACKUP'", "");print}'`

cp -prvf "$SNP_VAR" "$_BACKUP_SNAPSHOT_DIR$_SNP_PATH_TRIM";

done

echo "Snapshots created"