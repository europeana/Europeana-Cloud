#Migrator usage 

## 1. Usage example - migrate MCS tables to the newest version (requres empty keySpace)
> java -jar ./target/CassandraMigration-0.1-SNAPSHOT.jar -host localhost -keySpace ecloud_mcs -port 9042 -user cassandra -password cassandra -service MCS


## 2. To init migrations on already initiated schema (omit first migrations)

Create migrations tables

> CREATE TABLE ecloud_mcs.cassandra_migration_version (     version text PRIMARY KEY,
    checksum int,
    description text,
    execution_time int,
    installed_by text,
    installed_on timestamp,
    installed_rank int,
    script text,
    success boolean,
    type text,
    version_rank int
); 

> CREATE TABLE ecloud_mcs.cassandra_migration_version_counts (
    name text PRIMARY KEY,
    count counter
);

Populate migrations tables

> INSERT INTO ecloud_mcs.cassandra_migration_version (version, description , execution_time, installed_by , installed_on , installed_rank , script , success , type , version_rank ) 
VALUES ( '1','Initial MCS',10359,'cassandra',dateOf(now()),1,'migrations.service.mcs.V1__Initial_MCS',true,'JAVA_DRIVER',1);

> UPDATE ecloud_mcs.cassandra_migration_version_counts SET count = count + 1 WHERE name = 'installed_rank' ;