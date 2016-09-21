#Migrator usage 

## 1. Usage example - migrate MCS tables to the newest version (requres empty keySpace).

> java -jar ./target/CassandraMigration-0.1-SNAPSHOT.jar -host localhost -keySpace ecloud_mcs -port 9042 -user cassandra -password cassandra -service MCS


## 2. To migrate keySpace on already initiated schema (with omited first migration).

Instruction is valid for KeySpace schema with applied first migration (eg. V1__Initial_MCS). 

### 2.1. Create migrations tables

Create tables needed for migration tool.

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

### 2.2. Populate migrations tables

Insert manually data related to mark already applied migrations (eg. V1__Initial_MCS).  

> INSERT INTO ecloud_mcs.cassandra_migration_version (version, description , execution_time, installed_by , installed_on , installed_rank , script , success , type , version_rank ) 
VALUES ( '1','Initial MCS',10359,'cassandra',dateOf(now()),1,'migrations.service.mcs.V1__Initial_MCS',true,'JAVA_DRIVER',1);

> UPDATE ecloud_mcs.cassandra_migration_version_counts SET count = count + 1 WHERE name = 'installed_rank' ;

### 2.3. Invoke tool to apply rest migrations

Run migration tool for apply migrations to the newest schema (with omitted first migration marked as already applied).

> java -jar ./target/CassandraMigration-0.1-SNAPSHOT.jar -host localhost -keySpace ecloud_mcs -port 9042 -user cassandra -password cassandra -service MCS