CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE test_keyspace;
CREATE TABLE IF NOT EXISTS users(username varchar, password varchar, roles set<text>, PRIMARY KEY (username));
CREATE TABLE data_set_assignments_by_data_set_buckets (
                                                          object_id varchar,
                                                          bucket_id timeuuid,
                                                          rows_count counter,
                                                          PRIMARY KEY (object_id, bucket_id)
) WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';