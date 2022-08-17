package eu.europeana.cloud.service.dps.config;

public class JndiNames {

    private JndiNames() {
    }

    public static final String JNDI_KEY_KAFKA_BROKER = "/dps/kafka/brokerLocation";
    public static final String JNDI_KEY_KAFKA_GROUP_ID = "/dps/kafka/groupId";
    public static final String JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS = "/dps/zookeeper/address";

    public static final String JNDI_KEY_AAS_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    public static final String JNDI_KEY_AAS_CASSANDRA_PORT = "/aas/cassandra/port";
    public static final String JNDI_KEY_AAS_CASSANDRA_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    public static final String JNDI_KEY_AAS_CASSANDRA_USERNAME = "/aas/cassandra/user";
    public static final String JNDI_KEY_AAS_CASSANDRA_PASSWORD = "/aas/cassandra/password";

    public static final String JNDI_KEY_DPS_CASSANDRA_HOSTS = "/dps/cassandra/hosts";
    public static final String JNDI_KEY_DPS_CASSANDRA_PORT = "/dps/cassandra/port";
    public static final String JNDI_KEY_DPS_CASSANDRA_KEYSPACE = "/dps/cassandra/keyspace";
    public static final String JNDI_KEY_DPS_CASSANDRA_USERNAME = "/dps/cassandra/user";
    public static final String JNDI_KEY_DPS_CASSANDRA_PASSWORD = "/dps/cassandra/password";

    public static final String JNDI_KEY_TOPOLOGY_NAMELIST = "/dps/topology/nameList";
    public static final String JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS = "/dps/topology/availableTopics";
    public static final String JNDI_KEY_MCS_LOCATION = "/dps/mcsLocation";
    public static final String JNDI_KEY_UIS_LOCATION = "/dps/uisLocation";
    public static final String JNDI_KEY_APPLICATION_ID = "/dps/appId";
    public static final String JNDI_KEY_MACHINE_LOCATION = "/dps/machineLocation";
    public static final String JNDI_KEY_TOPOLOGY_USER = "/dps/topology/user";
    public static final String JNDI_KEY_TOPOLOGY_USER_PASSWORD = "/dps/topology/password";
}
