USE ecloud_mcs;
 
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
PRIMARY KEY ((provider_dataset_id, bucket_id), schema_id, cloud_id, version_id)
);

CREATE TABLE data_set_assignments_by_data_set_buckets (
  object_id varchar,
  bucket_id timeuuid,
  rows_count counter,
  PRIMARY KEY (object_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';

CREATE TABLE data_set_assignments_by_representations (
  cloud_id varchar,
  schema_id varchar,
  version_id timeuuid,
  provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */
  creation_date timestamp,
  PRIMARY KEY ((cloud_id, schema_id), version_id, provider_dataset_id)
);


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

CREATE TABLE data_set_assignments_by_revision_id_v1 (
       provider_id varchar,
       dataset_id varchar,
       bucket_id timeuuid,
       revision_provider_id varchar,
       revision_name varchar,
       revision_timestamp timestamp,
       representation_id varchar,
       cloud_id varchar,
       published boolean,
       acceptance boolean,
       mark_deleted boolean,
       PRIMARY KEY ((provider_id, dataset_id, bucket_id), revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id)
)WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';

CREATE TABLE data_set_assignments_by_revision_id_buckets (
      object_id varchar,
      bucket_id timeuuid,
      rows_count counter,
      PRIMARY KEY (object_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';

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


CREATE TABLE latest_provider_dataset_representation_revision (
    provider_id varchar,
    dataset_id varchar,
    cloud_id varchar,
    representation_id varchar,
    revision_timestamp timestamp,
    revision_name varchar,
    revision_provider varchar,
    version_id timeuuid,
    acceptance boolean,
    published boolean,
    mark_deleted boolean,
    PRIMARY KEY ((provider_id, dataset_id),representation_id,revision_name,revision_provider,mark_deleted,cloud_id)
);

CREATE TABLE latest_dataset_representation_revision_v1 (
    provider_id varchar,
    dataset_id varchar,
    bucket_id timeuuid,
    cloud_id varchar,
    representation_id varchar,
    revision_timestamp timestamp,
    revision_name varchar,
    revision_provider varchar,
    version_id timeuuid,
    acceptance boolean,
    published boolean,
    mark_deleted boolean,
    PRIMARY KEY ((provider_id, dataset_id, bucket_id),representation_id, revision_name, revision_provider, mark_deleted, cloud_id)
);

CREATE TABLE latest_dataset_representation_revision_buckets (
    object_id varchar,
    bucket_id timeuuid,
    rows_count counter,
    PRIMARY KEY (object_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';


CREATE TABLE latest_revisions_for_dataset_assignment (
       provider_id varchar,
       dataset_id varchar,
       representation_id varchar,
       revision_name varchar,
       revision_provider_id varchar,
       revision_timestamp timestamp,
       cloud_id varchar,
       version_id timeuuid,
       PRIMARY KEY ((provider_id, dataset_id, representation_id, cloud_id, revision_name, revision_provider_id))
)WITH comment='Stores information about representation version that has latest revision';

CREATE TABLE representation_revisions (
       cloud_id varchar,
       representation_id varchar,
       version_id timeuuid,
       revision_name varchar,
       revision_provider_id varchar,
       revision_timestamp timestamp,
       files map<varchar, text>, /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */
       PRIMARY KEY ((cloud_id, representation_id), revision_provider_id, revision_name, revision_timestamp, version_id)
)WITH CLUSTERING ORDER BY (revision_provider_id ASC, revision_name ASC, revision_timestamp DESC, version_id ASC) AND comment='Retrieve version_id based on the rest known fields of the primary key';

CREATE TABLE files_content (
    fileName varchar,
    data blob,
    PRIMARY KEY (fileName)
);