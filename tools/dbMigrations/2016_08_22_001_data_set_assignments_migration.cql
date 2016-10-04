COPY data_set_assignments TO 'data_set_assignments_dump';

DROP TABLE data_set_assignments;

CREATE TABLE ecloud_mcs.data_set_assignments (
	cloud_id text,
	schema_id text,
	provider_dataset_id text,
	version_id timeuuid,
	creation_date timestamp,
	PRIMARY KEY (cloud_id, schema_id, provider_dataset_id, version_id)
);

CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);

COPY data_set_assignments (cloud_id , schema_id , provider_dataset_id , creation_date , version_id ) FROM 'data_set_assignments_dump';





CREATE TABLE data_set_assignments_by_revision_id (
	   provider_dataset_id varchar, /* concatenation: provider_id | dataset_id - due to create index on it */
       revision_id varchar,
       representation_id varchar,
       cloud_id varchar,
       PRIMARY KEY ((provider_dataset_id , revision_id, representation_id),  cloud_id)
)WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';

CREATE INDEX data_set_assignments_by_revision_id_provider_dataset_id ON data_set_assignments_by_revision_id (provider_dataset_id);