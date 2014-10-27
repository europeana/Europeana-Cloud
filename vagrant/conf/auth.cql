CREATE USER ecloud_app WITH PASSWORD 'ecloud' NOSUPERUSER;
GRANT SELECT ON KEYSPACE ecloud_mcs TO ecloud_app;
GRANT MODIFY ON KEYSPACE ecloud_mcs TO ecloud_app;
 
CREATE USER ecloud_admin WITH PASSWORD 'ecloud' NOSUPERUSER;
GRANT ALTER ON KEYSPACE ecloud_mcs TO ecloud_admin;
GRANT CREATE ON KEYSPACE ecloud_mcs TO ecloud_admin;
GRANT DROP ON KEYSPACE ecloud_mcs TO ecloud_admin;
GRANT MODIFY ON KEYSPACE ecloud_mcs TO ecloud_admin;
GRANT SELECT ON KEYSPACE ecloud_mcs TO ecloud_admin;