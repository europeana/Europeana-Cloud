CREATE KEYSPACE IF NOT EXISTS ecloud_aas_tests WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE ecloud_aas_tests;

CREATE TABLE users(
            username varchar,
            password varchar,
            roles set<text>,
            locked boolean,
            PRIMARY KEY (username));

INSERT INTO users (username, password, roles) VALUES('Robin_Van_Persie', 'Feyenoord', {'ROLE_USER'});
INSERT INTO users (username, password, roles) VALUES('Cristiano', 'Ronaldo', {'ROLE_USER'});
INSERT INTO users (username, password, roles) VALUES('admin', 'admin', {'ROLE_ADMIN'});