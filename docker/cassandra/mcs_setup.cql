USE ecloud_mcs;
 
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
	PRIMARY KEY (cloud_id, schema_id, version_id, provider_dataset_id)
);
CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);
 
CREATE TABLE representation_versions (
       cloud_id varchar,
       schema_id varchar,
       version_id timeuuid,
       provider_id varchar,
       persistent boolean,
       files map<varchar, text>, /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
       revisions map<varchar, text>,
       creation_date timestamp,
       PRIMARY KEY (cloud_id, schema_id, version_id)
);
 
CREATE INDEX representations_provider_id ON representation_versions (provider_id);


CREATE TABLE data_set_assignments_by_revision_id (
       provider_id varchar,
       dataset_id varchar,
       revision_provider_id varchar,
       revision_name varchar,
       revision_timestamp timestamp,
       representation_id varchar,
       cloud_id varchar,
       published boolean,
       acceptance boolean,
       mark_deleted boolean,
       PRIMARY KEY ((provider_id, dataset_id), revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id)
)WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';

CREATE TABLE data_set_representation_names(
       provider_id varchar,
       dataset_id varchar,
       representation_names set<text>,
       PRIMARY KEY ((provider_id, dataset_id))
)WITH comment='Retrieve information about the representations supported in a providers dataset';