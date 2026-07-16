CREATE KEYSPACE junit_mcs WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE junit_mcs;

CREATE TABLE data_sets (
	provider_id varchar,
	dataset_id varchar,
	description text,
	creation_date timestamp,
	PRIMARY KEY (provider_id, dataset_id)
);

CREATE TABLE data_set_assignments_by_data_set (
  provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */
  bucket_id timeuuid,
  cloud_id varchar,
  schema_id varchar,
  version_id timeuuid,
  creation_date timestamp,
  mark_deleted boolean,
  PRIMARY KEY ((provider_dataset_id, bucket_id), schema_id, cloud_id, version_id)
);

CREATE TABLE data_set_assignments_by_data_set_buckets (
  object_id varchar,
  bucket_id timeuuid,
  rows_count counter,
  PRIMARY KEY (object_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';


CREATE TABLE representation_versions (
	cloud_id varchar,
	schema_id varchar,
	version_id timeuuid,
	provider_id varchar,
	persistent boolean,
	files map<varchar, text>,  /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
	creation_date timestamp,
    dataset_id varchar,
    mark_deleted boolean,
    annotations map<varchar, text>,
	PRIMARY KEY (cloud_id, schema_id, version_id)
);

CREATE TABLE datasets_buckets (
    provider_id varchar,
    dataset_id varchar,
    bucket_id timeuuid,
    rows_count counter,
    PRIMARY KEY (provider_id, dataset_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset';

CREATE TABLE files_content (
    fileName varchar,
    data blob,
    PRIMARY KEY (fileName)
);


CREATE TABLE files_content_v2 (
                               md5 uuid,
                               fileName varchar,
                               data blob STATIC,
                               updated TIMESTAMP STATIC,
                               PRIMARY KEY (md5, fileName)
);