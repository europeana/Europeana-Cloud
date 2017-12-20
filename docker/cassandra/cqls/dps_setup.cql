USE ecloud_dps;

CREATE TABLE basic_info (
    task_id bigint,
    topology_name varchar,
    expected_size int,
    processed_files_count int,
    state varchar,
    info varchar,
    start_time timestamp,
    finish_time timestamp,
    sent_time timestamp,
    errors int,
    PRIMARY KEY(task_id)
);

CREATE TABLE notifications (
    resource_num int,
    task_id bigint,
    topology_name varchar,
    resource varchar,
    state varchar,
    info_text text,
    additional_informations text,
    result_resource varchar,
    PRIMARY KEY(task_id,resource_num)
) WITH CLUSTERING ORDER BY (resource_num DESC);


CREATE TABLE error_notifications (
    task_id bigint,
    error_type timeuuid,
    error_message text,
    resource varchar,
    PRIMARY KEY((task_id,error_type),resource)
);

CREATE TABLE error_counters (
    task_id bigint,
    error_type timeuuid,
    error_count counter,
    PRIMARY KEY(task_id,error_type)
);