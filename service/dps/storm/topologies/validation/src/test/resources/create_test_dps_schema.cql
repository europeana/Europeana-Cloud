/*

   ! IMPORTANT NOTICE !
   Due to a bug in parsing by cassandraunit, statements must end with a semicolon directly followed by
   a newline character - particularly, there cannot be whitespace before line end

   This file assumes that it is executed in a particular keyspace context.
   In order to create a keyspace, you can use e.g.

   CREATE KEYSPACE ecloud_test
           WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
   USE ecloud_test;
*/

USE ecloud_test;
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
