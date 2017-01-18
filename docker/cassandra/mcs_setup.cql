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
	   provider_dataset_id varchar, /* concatenation: provider_id | dataset_id - due to create index on it */
       revision_id varchar,
       representation_id varchar,
       cloud_id varchar,
       PRIMARY KEY ((provider_dataset_id , revision_id, representation_id),  cloud_id)
)WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';

CREATE INDEX data_set_assignments_by_revision_id_provider_dataset_id ON data_set_assignments_by_revision_id (provider_dataset_id);

CREATE TABLE data_set_representation_names(
       provider_id varchar,
       dataset_id varchar,
       representation_names set<text>,
       PRIMARY KEY ((provider_id, dataset_id))
)WITH comment='Retrieve information about the representations supported in a providers dataset';

CREATE TABLE datasets_buckets (
    provider_id varchar,
    dataset_id varchar,
    bucket_id timeuuid,
    rows_count counter,
    PRIMARY KEY (provider_id, dataset_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset.';

CREATE TABLE provider_dataset_representation (
    provider_id varchar,
    dataset_id varchar,
    bucket_id timeuuid,
    cloud_id varchar,
    representation_id varchar,
    version_id timeuuid,
    revision_id varchar,
    revision_timestamp timestamp,
    acceptance boolean,
    published boolean,
    mark_deleted boolean,
    PRIMARY KEY ((provider_id, dataset_id, bucket_id), representation_id, revision_timestamp, cloud_id)
)WITH comment='Retrieve cloud Ids based on the revision_timestamp that is greater from a specific value. Used for full and incremental updates';

CREATE INDEX provider_dataset_representation_acceptance ON provider_dataset_representation (acceptance);

CREATE INDEX dataset_representation_published ON provider_dataset_representation (published);

CREATE INDEX provider_dataset_representation_mark_deleted ON provider_dataset_representation (mark_deleted);

CREATE INDEX provider_dataset_representation_revision_id ON provider_dataset_representation (revision_id);




CREATE TABLE latest_provider_dataset_representation_revision (
    provider_id varchar,
    dataset_id varchar,
    cloud_id varchar,
    representation_id varchar,
    revision_timestamp timestamp,
    revision_id varchar,
    version_id timeuuid,
    acceptance boolean,
    published boolean,
    mark_deleted boolean,
    PRIMARY KEY ((provider_id, dataset_id), representation_id,cloud_id)
);

CREATE INDEX latest_provider_dataset_representation_revision_acceptanc ON latest_provider_dataset_representation_revision (acceptance);

CREATE INDEX latest_provider_dataset_representation_revision_publishe ON latest_provider_dataset_representation_revision (published);

CREATE INDEX latest_provider_dataset_representation_revision_delete ON latest_provider_dataset_representation_revision (mark_deleted);

CREATE INDEX latest_provider_dataset_representation_revision_revision_id ON latest_provider_dataset_representation_revision (revision_id);

CREATE INDEX latest_provider_dataset_representation_revision_cloud_id ON latest_provider_dataset_representation_revision (revision_timestamp);