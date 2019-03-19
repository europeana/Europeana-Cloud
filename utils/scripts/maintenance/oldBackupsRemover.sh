#!/bin/bash
#
# This script removes old backups from backup machine:
# In this case 'old' means backups from the penultimate month
#

#exit script of first failed command
set -e

backupLocation=/mnt/backup
GREEN='\033[0;32m'
NC='\033[0m' # No Color

backupTimeInMonthsForDeletion=`date -d "-2 month" +"%Y-%m"`

echo  -e "${GREEN}Removing backup $backupTimeInMonthsForDeletion from backup machine${NC}"
rm -r $backupLocation/$backupTimeInMonthsForDeletion
echo  -e "${GREEN}Backup removed${NC}"