CREATE KEYSPACE IF NOT EXISTS junit_aas WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE junit_aas;

INSERT INTO users (username, password, roles) VALUES('admin', 'ecloud_admin', {'ROLE_ADMIN'});

INSERT INTO users (username, password, roles) VALUES('user', 'ecloud_user', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('ic_user', 'ic_user', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('xslt_user', 'xslt_user', {'ROLE_USER'});