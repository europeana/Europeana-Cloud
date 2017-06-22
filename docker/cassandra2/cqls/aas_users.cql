USE ecloud_aas;

INSERT INTO users (username, password, roles) VALUES('admin', 'ecloud_admin', {'ROLE_ADMIN'});

INSERT INTO users (username, password, roles) VALUES('user', 'ecloud_user', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('ic_user', 'ic_user', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('xslt_user2', 'xslt_user', {'ROLE_USER'});