CREATE KEYSPACE IF NOT EXISTS uis_test4 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE uis_test4;
CREATE TABLE IF NOT EXISTS data_providers (provider_id varchar PRIMARY KEY,properties map<varchar, text>, data_sets map<varchar, varchar>, creation_date timestamp, partition_key int, active boolean);
CREATE TABLE IF NOT EXISTS Cloud_Id(cloud_id varchar, provider_id varchar, record_id varchar, PRIMARY KEY (cloud_id, provider_id,record_id));
CREATE TABLE IF NOT EXISTS Provider_Record_Id(provider_id varchar, bucket_id timeuuid, record_id varchar, cloud_id varchar, PRIMARY KEY (provider_id, bucket_id, record_id));
CREATE TABLE IF NOT EXISTS provider_record_id_buckets (object_id text, bucket_id timeuuid, rows_count counter, PRIMARY KEY (object_id, bucket_id));