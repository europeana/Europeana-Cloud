package eu.europeana.cloud.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CassandraTestInstance {
    public static final int PORT = 19142;
    public static final String CASSANDRA_CONFIG_FILE = EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE;
    static final Logger LOGGER = LoggerFactory.getLogger(CassandraTestInstance.class);
    private static volatile CassandraTestInstance instance;
    private static volatile Multimap<String, String> keyspaceDescriptor = Multimaps.synchronizedMultimap
            (HashMultimap.<String,String>create(2, 1));
    private final Cluster cluster;
    private Session session;
    private static final long CASSANDRA_STARTUP_TIMEOUT = 20000L;

    private CassandraTestInstance() {
        if (instance != null) {
            throw new IllegalStateException("Already initialized.");
        }
        try {
            LOGGER.info("Starting embedded Cassandra");
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(CassandraTestInstance.CASSANDRA_CONFIG_FILE, CASSANDRA_STARTUP_TIMEOUT);
            cluster = Cluster.builder().addContactPoint("localhost").withPort(CassandraTestInstance.PORT).build();
            LOGGER.info("embedded Cassandra initialized.");
        } catch (Exception e) {
            LOGGER.error("Cannot start embedded Cassandra!", e);
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
            throw new RuntimeException("Cannot start embedded Cassandra!", e);
        }
    }

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

    private void initKeyspaceIfNeeded(String keyspaceSchemaCql, String keyspace) {
        if (!keyspaceDescriptor.containsKey(keyspace) ||
                !keyspaceDescriptor.get(keyspace).contains(keyspaceSchemaCql)) {
            initKeyspace(keyspaceSchemaCql, keyspace);
        } else {
            LOGGER.info("embedded Cassandra keyspace " + keyspace + " is already initialized.");
        }
    }

    private void initKeyspace(String keyspaceSchemaCql, String keyspace) {
        LOGGER.info("Initializing embedded Cassandra keyspace " + keyspace + " ...");
        Session session = cluster.newSession();
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(new ClassPathCQLDataSet(keyspaceSchemaCql, keyspace));
        session.close();
        this.session = cluster.connect(keyspace);
        keyspaceDescriptor.put(keyspace, keyspaceSchemaCql);
        LOGGER.info("embedded Cassandra keyspace " + keyspace + " initialized.");
    }

    public synchronized void truncateAllData() {
        LOGGER.info("Truncating all not empty tables");
        for (String keyspaceName : keyspaceDescriptor.keySet()) {
            truncateKeyspace(keyspaceName);
        }
    }

    private void truncateKeyspace(String keyspaceName) {
        final ResultSet rs = session
                .execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
                        keyspaceName
                        + "';");
        for (Row r : rs.all()) {
            String tableName = r.getString("columnfamily_name");
            truncateIfNotEmpty(tableName);
        }
    }

    private void truncateIfNotEmpty(String tableName) {
        ResultSet rows = session
                .execute("SELECT * FROM " + tableName + " LIMIT 1;");
        if (rows.one() == null) {
            LOGGER.info("embedded Cassandra table " + tableName + " - is empty");
        } else {
            LOGGER.info("embedded Cassandra tuncating table: " + tableName);
            session.execute("TRUNCATE " + tableName);
        }
    }
}