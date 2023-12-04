CREATE KEYSPACE ecloud_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE ecloud_test;

CREATE TABLE task_info (
    task_id                                bigint,
    topology_name                          varchar,
    state                                  varchar,
    state_description                      varchar,
    sent_timestamp                         timestamp,
    start_timestamp                        timestamp,
    finish_timestamp                       timestamp,
    expected_records_number                int,
    processed_records_count                int,
    ignored_records_count                  int,
    deleted_records_count                  int,
    processed_errors_count                 int,
    deleted_errors_count                   int,
    expected_post_processed_records_number int,
    post_processed_records_count           int,
    definition                             text,
    PRIMARY KEY (task_id)
);

CREATE TABLE task_diagnostic_info (
    task_id                            bigint,
    started_records_count              int,
    records_retry_count                int,
    queued_time                        timestamp,
    start_on_storm_time                timestamp,
    last_record_finished_on_storm_time timestamp,
    finish_on_storm_time               timestamp,
    post_processing_start_time         timestamp,
    PRIMARY KEY (task_id)
);

CREATE TABLE notifications (
    task_id bigint,
    bucket_number int,
    resource_num int,
    topology_name varchar,
    resource varchar,
    state varchar,
    info_text text,
    additional_information map<text,text>,
    result_resource varchar,
    PRIMARY KEY((task_id,bucket_number),resource_num)
) WITH CLUSTERING ORDER BY (resource_num DESC)
  AND default_time_to_live = 5184000;


CREATE TABLE error_notifications (
    task_id bigint,
    error_type timeuuid,
    error_message text,
    resource varchar,
    additional_informations text,
    PRIMARY KEY((task_id,error_type),resource)
);

CREATE TABLE error_types (
    task_id bigint,
    error_type timeuuid,
    error_count int,
    PRIMARY KEY(task_id,error_type)
);

CREATE TABLE general_statistics (
    task_id bigint,
    parent_xpath varchar,
    node_xpath varchar,
    occurrence counter,
    PRIMARY KEY(task_id, parent_xpath, node_xpath)
);

CREATE TABLE node_statistics (
    task_id bigint,
    node_xpath varchar,
    node_value varchar,
    occurrence counter,
    PRIMARY KEY((task_id, node_xpath), node_value)
);

CREATE TABLE attribute_statistics (
    task_id bigint,
    node_xpath varchar,
    node_value varchar,
    attribute_name varchar,
    attribute_value varchar,
    occurrence counter,
    PRIMARY KEY((task_id, node_xpath, node_value), attribute_name, attribute_value)
);

CREATE TABLE statistics_reports (
task_id bigint,
report_data blob,
PRIMARY KEY(task_id)
);

CREATE TABLE tasks_by_task_state(
    state varchar,
    topology_name varchar,
    task_id bigint,
    application_id varchar,
    topic_name varchar,
    start_time timestamp,
    PRIMARY KEY(state, topology_name, task_id)
);

CREATE TABLE processed_records (
    task_id bigint,
    bucket_number int,
    record_id varchar,
    attempt_number int,
    dst_identifier varchar,
    topology_name varchar,
    state varchar,
    start_time     timestamp,
    info_text text,
    additional_informations text,
    PRIMARY KEY((task_id, bucket_number), record_id)
) WITH default_time_to_live = 5184000;

create table harvested_records(
    metis_dataset_id varchar,
    bucket_number    int,
    record_local_id  varchar,
    latest_harvest_date timestamp,
    latest_harvest_md5 uuid,
    preview_harvest_date timestamp,
    preview_harvest_md5 uuid,
    published_harvest_date timestamp,
    published_harvest_md5 uuid,
    PRIMARY KEY ((metis_dataset_id, bucket_number), record_local_id)
);