USE ecloud_aas;

INSERT INTO users (username, password, roles) VALUES('admin', 'ecloud_admin', {'ROLE_ADMIN'});

INSERT INTO users (username, password, roles) VALUES('user', 'ecloud_user', {'ROLE_USER'});