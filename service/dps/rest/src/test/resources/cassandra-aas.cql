CREATE KEYSPACE IF NOT EXISTS ecloud_aas_tests WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE ecloud_aas_tests;

CREATE TABLE IF NOT EXISTS users(
            username varchar,
            password varchar,
            roles set<text>,
            locked boolean,
            PRIMARY KEY (username));

INSERT INTO users (username, password, roles) VALUES('Robin_Van_Persie', '$2a$04$QPmt/FlIU5DVQ3Ls4xAr9u6zMeJZVcJw3/H1TpCpTm2j9F.JBiwwW', {'ROLE_USER'});
INSERT INTO users (username, password, roles) VALUES('Cristiano', '$2a$04$RD1mMlpVzOr3qr.tunzweeikDEbCHBImEfcwy5/C6hkQq4QrtVR8e', {'ROLE_USER'});
INSERT INTO users (username, password, roles) VALUES('admin', '$2a$04$hh7IGdQPKBsauzYLS13PC.wR5sJAcOaP57AGPWjQd23gCls4q5NKa', {'ROLE_ADMIN'});