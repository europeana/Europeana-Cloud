/* 

   ! IMPORTANT NOTICE !
   Due to a bug in parsing by cassandraunit, statements must end with a semicolon directly followed by 
   a newline character - particularly, there cannot be whitespace before line end 

   This file assumes that it is executed in a particular keyspace context. 
   In order to create a keyspace, you can use e.g.
  
   CREATE KEYSPACE ecloud_test
           WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};  
   USE ecloud_test; 
*/
CREATE KEYSPACE IF NOT EXISTS integration_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE integration_test;

CREATE TABLE data_providers (provider_id varchar PRIMARY KEY,properties map<varchar, text>, data_sets map<varchar, varchar>, creation_date timestamp);
CREATE TABLE Cloud_Id(cloud_id varchar, provider_id varchar, record_id varchar, deleted boolean, PRIMARY KEY (cloud_id, provider_id,record_id));
CREATE INDEX deleted_records ON Cloud_Id(deleted);
CREATE TABLE Provider_Record_Id(provider_id varchar, record_id varchar, cloud_id varchar, deleted boolean, PRIMARY KEY (provider_id,record_id));
CREATE INDEX record_deleted on Provider_Record_Id(deleted);

CREATE TABLE data_sets (
	provider_id varchar,
	dataset_id varchar,
	description text,
	creation_date timestamp, 
	PRIMARY KEY (provider_id, dataset_id)
);

CREATE TABLE data_set_assignments (
	provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */
	cloud_id varchar,
	schema_id varchar,
	version_id timeuuid,
	creation_date timestamp,	
	PRIMARY KEY (cloud_id, schema_id, provider_dataset_id)
);
CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);


CREATE TABLE representation_versions (
	cloud_id varchar,
	schema_id varchar,
	version_id timeuuid,
	provider_id varchar,
	persistent boolean,
	files map<varchar, text>,  /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
	creation_date timestamp,
	PRIMARY KEY (cloud_id, schema_id, version_id)
);
CREATE INDEX representations_provider_id ON representation_versions (provider_id);