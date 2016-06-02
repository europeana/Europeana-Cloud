USE ecloud_dps ;

CREATE TABLE basic_info (
task_id bigint,
topology_name varchar,
expected_size int,
state varchar,
info varchar,
sent_time timestamp,
start_time timestamp,
finish_time timestamp,
PRIMARY KEY(task_id, topology_name)
);

CREATE TABLE notifications (
    task_id bigint,
    topology_name varchar,
    resource varchar,
    state varchar,
    info_text text,
    additional_informations text,
    result_resource varchar,
    PRIMARY KEY(task_id, topology_name, resource)
);