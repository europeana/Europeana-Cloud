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
        deleted boolean,
        PRIMARY KEY (cloud_id, provider_id,record_id)
);
CREATE INDEX deleted_records ON Cloud_Id(deleted);
CREATE TABLE Provider_Record_Id(
        provider_id varchar,
        record_id varchar,
        cloud_id varchar,
        deleted boolean,
        PRIMARY KEY (provider_id,record_id)
);
CREATE INDEX record_deleted on Provider_Record_Id(deleted);