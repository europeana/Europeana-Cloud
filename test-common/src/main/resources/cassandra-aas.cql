CREATE KEYSPACE IF NOT EXISTS aas_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE aas_test;

CREATE TABLE users(
            username varchar,
            password varchar,
            roles set<text>,
            locked boolean,
            PRIMARY KEY (username));

INSERT INTO users (username, password, roles) VALUES('Robin_Van_Persie', 'Feyenoord', {'ROLE_USER'});
INSERT INTO users (username, password, roles) VALUES('Cristiano', 'Ronaldo', {'ROLE_USER'});
INSERT INTO users (username, password, roles) VALUES('admin', 'admin', {'ROLE_ADMIN'});