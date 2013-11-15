CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE %s;
CREATE TABLE IF NOT EXISTS Cloud_Id(cloud_id varchar, provider_id varchar, record_id varchar, deleted boolean, PRIMARY KEY (cloud_id, provider_id,record_id));
CREATE INDEX IF NOT EXISTS deleted_records ON Cloud_Id(deleted);
CREATE TABLE IF NOT EXISTS Provider_Record_Id(provider_id varchar, record_id varchar, cloud_id varchar, deleted boolean, PRIMARY KEY (provider_id,record_id));
CREATE INDEX IF NOT EXISTS record_deleted on Provider_Record_Id(deleted);