USE ecloud_aas;

INSERT INTO users (username, password, roles) VALUES('admin', '$2a$04$TsJN7o/K8LjIYmO7Jt4nCOtmxO7l54OjrAlqFs8fdGu4TrpVZJla.', {'ROLE_ADMIN'});

INSERT INTO users (username, password, roles) VALUES('user', '$2a$04$eSFU4VmDU.acGH/kvmZ2iuLkXA8f78AQG0WJfDcN/wcp6ArSwGrES', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('ic_user', '$2a$04$yfGYYtqQ/32bYbr2SODuSe7MevWbeLwP4rD6U657tLAwnF7G1FHF6', {'ROLE_USER'});

INSERT INTO users (username, password, roles) VALUES('xslt_user', '$2a$04$hWDte0GUbF5B.B5oN4dGf.6JxurY8GOEwh86FiguEKtBy9kmv1e.O', {'ROLE_USER'});