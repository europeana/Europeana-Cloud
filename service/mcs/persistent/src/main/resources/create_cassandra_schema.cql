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
	PRIMARY KEY (cloud_id, schema_id, provider_dataset_id, version_id)
);
CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);


CREATE TABLE representation_versions (
	cloud_id varchar,
	schema_id varchar,
	version_id timeuuid,
	provider_id varchar,
	persistent boolean,
	files map<varchar, text>,  /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
	revisions map<varchar, text>,
	creation_date timestamp,
	PRIMARY KEY (cloud_id, schema_id, version_id)
);
CREATE INDEX representations_provider_id ON representation_versions (provider_id);

CREATE TABLE data_set_representation_names(
       provider_id varchar,
       dataset_id varchar,
       representation_names set<text>,
       PRIMARY KEY ((provider_id, dataset_id))
)WITH comment='Retrieve information about the representations supported in a provider’s dataset';

CREATE TABLE representation_revisions (
       cloud_id varchar,
       representation_id varchar,
       version_id timeuuid,
       revision_id varchar,
       provider_id varchar,
       files map<varchar, text>, /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
       PRIMARY KEY ((cloud_id, representation_id), revision_id, version_id)
)WITH comment='Retrieve version_id based on the rest known fields of the primary key';
