USE ecloud_uis;

CREATE TABLE data_providers (
        provider_id varchar PRIMARY KEY,
        properties map<varchar, text>,
        data_sets map<varchar, varchar>,
        creation_date timestamp,
        partition_key int,
        active boolean
);

CREATE TABLE Cloud_Id (
        cloud_id varchar,
        provider_id varchar,
        record_id varchar,
        PRIMARY KEY (cloud_id, provider_id,record_id)
);

CREATE TABLE Provider_Record_Id(
        provider_id varchar,
        bucket_id timeuuid,
        record_id varchar,
        cloud_id varchar,
        PRIMARY KEY ((provider_id,bucket_id),record_id)
);

CREATE TABLE provider_record_id_buckets (
    object_id varchar, /*provider id*/
    bucket_id timeuuid,
    rows_count counter,
   PRIMARY KEY (object_id, bucket_id)
 ) WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';