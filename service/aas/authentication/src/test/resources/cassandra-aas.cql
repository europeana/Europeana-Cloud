CREATE KEYSPACE IF NOT EXISTS aas_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE aas_test;
CREATE TABLE IF NOT EXISTS users(username varchar, password varchar, PRIMARY KEY (username));