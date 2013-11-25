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
	properties map<varchar, text>, /* properties such as organization name, website url etc. */
	data_sets map<varchar, varchar>, /* identifier -> description of data set */
	creation_date timestamp	
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
	version timeuuid,
	provider_id varchar,
	persistent boolean,
	files map<varchar, text>,  /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
	creation_date timestamp,
	PRIMARY KEY (cloud_id, schema_id, version)
);