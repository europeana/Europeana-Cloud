package eu.europeana.cloud.test;

import com.datastax.driver.core.*;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class CassandraTestInstance {
    private static final int PORT = 19142;
    private static final String CASSANDRA_CONFIG_FILE = EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE;
    private static final long CASSANDRA_STARTUP_TIMEOUT = 20000L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTestInstance.class);

    private static volatile CassandraTestInstance instance;
    private static volatile Map<String, Session> keyspaceSessions =
            Collections.synchronizedMap(new HashMap<String, Session>());
    private final Cluster cluster;

    private CassandraTestInstance() {
        if (instance != null) {
            throw new IllegalStateException("Already initialized.");
        }
        try {
            LOGGER.info("Starting embedded Cassandra");
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(CassandraTestInstance.CASSANDRA_CONFIG_FILE,
                    CASSANDRA_STARTUP_TIMEOUT);
            cluster = buildClusterWithConsistencyLevel(ConsistencyLevel.ALL);
            LOGGER.info("embedded Cassandra initialized.");
        } catch (Exception e) {
            LOGGER.error("Cannot start embedded Cassandra!", e);
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
            throw new RuntimeException("Cannot start embedded Cassandra!", e);
        }
    }

    private Cluster buildClusterWithConsistencyLevel(ConsistencyLevel level) {
        QueryOptions options = new QueryOptions().setConsistencyLevel(level);
        return Cluster.builder().addContactPoints("localhost").withPort(CassandraTestInstance.PORT)
                .withProtocolVersion(ProtocolVersion.V3)
                .withQueryOptions(options)
                .withTimestampGenerator(new AtomicMonotonicTimestampGenerator()).build();
    }

    /**
     * Thread safe singleton of Cassandra test instance with initialized keyspace.
     * @param keyspaceSchemaCql cql file of keyspace definition
     * @param keyspace keyspace name
     * @return cassandra test instance
     */
    public static CassandraTestInstance getInstance(String keyspaceSchemaCql, String keyspace) {
        CassandraTestInstance result = instance;
        if (result == null) {
            synchronized (CassandraTestInstance.class) {
                result = instance;
                if (result == null) {
                    instance = result = new CassandraTestInstance();
                }
            }
        }
        instance.initKeyspaceIfNeeded(keyspaceSchemaCql, keyspace);
        return result;
    }

    /**
     * Truncate all tables from all keyspaces.
     * @param hard if is true then empty tables are truncated (this slow down process because it require disk flushes)
     */
    public static synchronized void truncateAllData(boolean hard) {
        LOGGER.info(keyspaceSessions.toString());
        for (String keyspaceName : keyspaceSessions.keySet()) {
            if (hard) {
                LOGGER.warn("Truncating all tables! Operation is slow use CassandraTestInstance.truncateAllData" +
                        "(false).");
                truncateAllKeyspaceTables(keyspaceName);
            } else {
                LOGGER.info("Truncating all not empty tables!");
                truncateAllNotEmptyKeyspaceTables(keyspaceName);
            }
        }
    }

    private static void truncateAllKeyspaceTables(String keyspaceName) {
        Session session = keyspaceSessions.get(keyspaceName);
        final ResultSet rs = session
                .execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
                        keyspaceName
                        + "';");
        for (Row r : rs.all()) {
            String tableName = r.getString("columnfamily_name");
            LOGGER.info("embedded Cassandra tuncating table: " + tableName);
            session.execute("TRUNCATE " + tableName);
        }
    }

    private static void truncateAllNotEmptyKeyspaceTables(String keyspaceName) {
        Session session = keyspaceSessions.get(keyspaceName);
        final ResultSet rs = session
                .execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
                        keyspaceName + "';");
        for (Row r : rs.all()) {
            String tableName = r.getString("columnfamily_name");
            ResultSet rows = session
                    .execute("SELECT * FROM " + tableName + " LIMIT 1;");
            if (rows.one() == null) {
                LOGGER.info("embedded Cassandra keyspace" + " table:" + tableName + " - is empty");
            } else {
                LOGGER.info("embedded Cassandra tuncating table: " + tableName);
                session.execute("TRUNCATE " + tableName);
            }
        }
    }

    /**
     * Print how many data is in each table.
     */
    public static synchronized void print() {
        LOGGER.info(keyspaceSessions.toString());
        for (String keyspaceName : keyspaceSessions.keySet()) {
            final ResultSet rs = keyspaceSessions.get(keyspaceName)
                    .execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
                            keyspaceName
                            + "';");
            for (Row r : rs.all()) {
                String tableName = r.getString("columnfamily_name");
                Session session = keyspaceSessions.get(keyspaceName);
                ResultSet rows = session
                        .execute("SELECT * FROM " + tableName + ";");
                LOGGER.info("keyspace : " + keyspaceName + ", table : " + tableName + " have rows : " + rows
                        .getAvailableWithoutFetching());
            }
        }
    }

    private void initKeyspaceIfNeeded(String keyspaceSchemaCql, String keyspace) {
        if (!keyspaceSessions.containsKey(keyspace)) {
            initKeyspace(keyspaceSchemaCql, keyspace);
        } else {
            LOGGER.info("embedded Cassandra keyspace " + keyspace + " is already initialized.");
        }
    }

    /**
     * Clean embedded Casandra and throw out keyspaces.
     */
    public synchronized void clean() {
        keyspaceSessions.clear();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    private void initKeyspace(String keyspaceSchemaCql, String keyspace) {
        LOGGER.info("Initializing embedded Cassandra keyspace " + keyspace + " ...");
        applyCQL(keyspaceSchemaCql, keyspace);
        Session session = cluster.connect(keyspace);
        keyspaceSessions.put(keyspace, session);
        LOGGER.info("embedded Cassandra keyspace " + keyspace + " initialized.");
    }

    private void applyCQL(String keyspaceSchemaCql, String keyspace) {
        Session tempSession = cluster.newSession();
        CQLDataLoader dataLoader = new CQLDataLoader(tempSession);
        dataLoader.load(new ClassPathCQLDataSet(keyspaceSchemaCql, keyspace));
        tempSession.close();
    }
}