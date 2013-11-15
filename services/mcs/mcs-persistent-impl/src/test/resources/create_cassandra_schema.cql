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

CREATE TABLE data_providers (
	provider_id varchar PRIMARY KEY,
	properties map<varchar, text>,
	creation_date timestamp	
);

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
	version varchar,
	creation_date timestamp,	
	PRIMARY KEY (cloud_id, schema_id, provider_dataset_id)
);
CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);


CREATE TABLE representations (
	cloud_id varchar,
	schema_id varchar,
	version varchar,
	provider_id varchar,
	persistent boolean,
	creation_date timestamp,
	PRIMARY KEY (cloud_id, schema_id, version)
);
CREATE INDEX representations_schema_id ON representations (schema_id);
CREATE INDEX representations_provider_id ON representations (provider_id);

CREATE TABLE representations_latest_versions (
	cloud_id varchar,
	schema_id varchar,
	version varchar,
	provider_id varchar,
	persistent boolean,
	PRIMARY KEY (cloud_id, schema_id, persistent)
);

CREATE TABLE files (
	representation_id varchar, /* concatenation: cloud_id | schema_id | version */
	file_name varchar,
	mime_type varchar,
	content_md5 varchar,
	content_length int,
	last_modification_date timestamp,
	PRIMARY KEY (representation_id, file_name)
);