
CREATE KEYSPACE ecloud_uis
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
CREATE KEYSPACE ecloud_mcs
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
CREATE KEYSPACE ecloud_aas
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 
//create users mcs
CREATE USER ecloud_mcs WITH PASSWORD 'password' NOSUPERUSER;
GRANT SELECT ON KEYSPACE ecloud_mcs TO ecloud_mcs;
GRANT MODIFY ON KEYSPACE ecloud_mcs TO ecloud_mcs;
  
CREATE USER ecloud_admin_mcs WITH PASSWORD 'password' NOSUPERUSER;
GRANT ALTER ON KEYSPACE ecloud_mcs TO ecloud_admin_mcs;
GRANT CREATE ON KEYSPACE ecloud_mcs TO ecloud_admin_mcs;
GRANT DROP ON KEYSPACE ecloud_mcs TO ecloud_admin_mcs;
GRANT MODIFY ON KEYSPACE ecloud_mcs TO ecloud_admin_mcs;
GRANT SELECT ON KEYSPACE ecloud_mcs TO ecloud_admin_mcs;
 
//crate users uis
 
CREATE USER ecloud_uis WITH PASSWORD 'password' NOSUPERUSER;
GRANT SELECT ON KEYSPACE ecloud_uis TO ecloud_uis;
GRANT MODIFY ON KEYSPACE ecloud_uis TO ecloud_uis;
  
CREATE USER ecloud_admin_uis WITH PASSWORD 'password' NOSUPERUSER;
GRANT ALTER ON KEYSPACE ecloud_uis TO ecloud_admin_uis;
GRANT CREATE ON KEYSPACE ecloud_uis TO ecloud_admin_uis;
GRANT DROP ON KEYSPACE ecloud_uis TO ecloud_admin_uis;
GRANT MODIFY ON KEYSPACE ecloud_uis TO ecloud_admin_uis;
GRANT SELECT ON KEYSPACE ecloud_uis TO ecloud_admin_uis;
 
//crate users aas
CREATE USER ecloud_aas WITH PASSWORD 'password' NOSUPERUSER;
GRANT SELECT ON KEYSPACE ecloud_aas TO ecloud_aas;
GRANT MODIFY ON KEYSPACE ecloud_aas TO ecloud_aas;
  
CREATE USER ecloud_admin_aas WITH PASSWORD 'password' NOSUPERUSER;
GRANT ALTER ON KEYSPACE ecloud_aas TO ecloud_admin_aas;
GRANT CREATE ON KEYSPACE ecloud_aas TO ecloud_admin_aas;
GRANT DROP ON KEYSPACE ecloud_aas TO ecloud_admin_aas;
GRANT MODIFY ON KEYSPACE ecloud_aas TO ecloud_admin_aas;
GRANT SELECT ON KEYSPACE ecloud_aas TO ecloud_admin_aas;
