CREATE KEYSPACE IF NOT EXISTS junit_aas WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE junit_aas;

INSERT INTO users (username, password, roles) VALUES('admin', '$2a$04$LbU.P.wAjzG2PEzxbqDcHerhrPSF3j/1v8n6Cbdy/z8sO6ua.fLLO', {'ROLE_ADMIN'});

INSERT INTO users (username, password, roles) VALUES('user', '$2a$04$SQg2gKSdtZx7V69WgzP3jO4CJNRy.QD3Q4G2lfGOqseu6h6cDmEeu', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('ic_user', '$2a$04$yfGYYtqQ/32bYbr2SODuSe7MevWbeLwP4rD6U657tLAwnF7G1FHF6', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('xslt_user', '$2a$04$hWDte0GUbF5B.B5oN4dGf.6JxurY8GOEwh86FiguEKtBy9kmv1e.O', {'ROLE_USER'});