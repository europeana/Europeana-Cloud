#!/bin/bash
#
# This script will grant permissions to given keyspace for given user
#
# Example usage:
# ./db_permissions_granter.sh 127.0.0.1 keyspaceName dbUserName dbUserPassword userName

cassandraLocation=$1
keyspaceName=$2
dbUser=$3
dbPassword=$4
userName=$5

echo "Granting permissions to keyspace $keyspaceName for $userName on $cassandraLocation"
echo "Granting ALTER permissions"
cqlsh $cassandraLocation -u $dbUser -p $dbPassword -e "GRANT ALTER ON KEYSPACE $keyspaceName TO $userName;"
echo "Granting CREATE permissions"
cqlsh $cassandraLocation -u $dbUser -p $dbPassword -e "GRANT CREATE ON KEYSPACE $keyspaceName TO $userName;"
echo "Granting DROP permissions"
cqlsh $cassandraLocation -u $dbUser -p $dbPassword -e "GRANT DROP ON KEYSPACE $keyspaceName TO $userName;"
echo "Granting MODIFY permissions"
cqlsh $cassandraLocation -u $dbUser -p $dbPassword -e "GRANT MODIFY ON KEYSPACE $keyspaceName TO $userName;"
echo "Granting SELECT permissions"
cqlsh $cassandraLocation -u $dbUser -p $dbPassword -e "GRANT SELECT ON KEYSPACE $keyspaceName TO $userName;"
echo "Procedure finished"
