package eu.europeana.cloud.test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.sleep;

public final class CassandraTestInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTestInstance.class);

    private static volatile CassandraTestInstance instance;
    private static volatile Map<String, Session> keyspaceSessions =
            Collections.synchronizedMap(new HashMap<>());
    private final Cluster cluster;
    private final CassandraContainer container;

    private CassandraTestInstance() {
        if (instance != null) {
            throw new IllegalStateException("Already initialized.");
        }
        try {
            LOGGER.info("Starting Cassandra container in docker");
            container = new CassandraContainer("cassandra:3.11.2");
            container.start();
            cluster = container.getCluster();

            LOGGER.info("Cassandra container initialized.");
        } catch (Exception e) {
            LOGGER.error("Cannot start Cassandra container!", e);
            throw new RuntimeException("Cannot start embedded Cassandra!", e);
        }
    }

    public static int getPort() {
        return instance.container.getMappedPort(9042);
    }

    /**
     * Thread safe singleton of Cassandra test instance with initialized keyspace.
     *
     * @param keyspaceSchemaCql cql file of keyspace definition
     * @param keyspace          keyspace name
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
     *
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

    public static Session getSession(String keyspace) {
        return keyspaceSessions.get(keyspace);
    }

    private static void truncateAllKeyspaceTables(String keyspaceName) {
        Session session = keyspaceSessions.get(keyspaceName);
        final ResultSet rs = session
                .execute("SELECT table_name from system.tables where keyspace_name='" +
                        keyspaceName
                        + "';");
        for (Row r : rs.all()) {
            String tableName = r.getString("columnfamily_name");
            LOGGER.info("embedded Cassandra tuncating table: {}", tableName);
            session.execute("TRUNCATE " + tableName);
        }
    }

    private static void truncateAllNotEmptyKeyspaceTables(String keyspaceName) {
        Session session = keyspaceSessions.get(keyspaceName);
        final ResultSet rs = session
                .execute("SELECT table_name from system_schema.tables where keyspace_name='" +
                        keyspaceName + "';");
        for (Row r : rs.all()) {
            String tableName = r.getString("table_name");
            ResultSet rows = session
                    .execute("SELECT * FROM " + tableName + " LIMIT 1;");
            if (rows.one() == null) {
                LOGGER.info("embedded Cassandra keyspace table:{} - is empty", tableName);
            } else {
                LOGGER.info("embedded Cassandra tuncating table: {}", tableName);
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
            Session session = keyspaceSessions.get(keyspaceName);
            final ResultSet rs = session.execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
                    keyspaceName + "';");

            for (Row r : rs.all()) {
                String tableName = r.getString("columnfamily_name");
                ResultSet rows = session
                        .execute("SELECT * FROM " + tableName + ";");
                LOGGER.info("keyspace : {}, table : {}  have rows : {} ", keyspaceName, tableName, rows.getAvailableWithoutFetching());
            }
        }
    }

    public void initKeyspaceIfNeeded(String keyspaceSchemaCql, String keyspace) {
        if (!keyspaceSessions.containsKey(keyspace)) {
            initKeyspace(keyspaceSchemaCql, keyspace);
        } else {
            LOGGER.info("embedded Cassandra keyspace {} is already initialized.", keyspace);
        }
    }

    /**
     * Clean embedded Casandra and throw out keyspaces.
     */
    public synchronized void clean() {
        keyspaceSessions.clear();
    }

    private void initKeyspace(String keyspaceSchemaCql, String keyspace) {
        LOGGER.info("Initializing embedded Cassandra keyspace {} ...", keyspace);
        applyCQL(keyspaceSchemaCql);
        Session session = cluster.connect(keyspace);
        keyspaceSessions.put(keyspace, session);
        LOGGER.info("embedded Cassandra keyspace {} initialized.", keyspace);
    }

    private void applyCQL(String keyspaceSchemaCql) {
        try (Session tempSession = cluster.newSession()) {
            String[] statements = StringUtils.split(IOUtils.toString(getClass().getClassLoader().getResourceAsStream(keyspaceSchemaCql)), ";");
            Arrays.stream(statements).map(statement -> StringUtils.normalizeSpace(statement) + ";").forEach(tempSession::execute);
        } catch (IOException e) {
            LOGGER.error("Unable to load data to Cassandra", e);
            throw new RuntimeException("Unable to load data to Cassandra");
        }
    }


    private static final class TestRetryPolicy implements RetryPolicy {
        public static final Logger log = LoggerFactory.getLogger(TestRetryPolicy.class);
        private final double maxReadNbRetry;
        private final double maxWriteNbRetry;
        private final double maxUnavailableNbRetry;
        private final long waitRetryTime;

        TestRetryPolicy(double maxReadNbRetry, double maxWriteNbRetry, double maxUnavailableNbRetry,
                        long waitRetryTime) {
            this.maxReadNbRetry = maxReadNbRetry;
            this.maxWriteNbRetry = maxWriteNbRetry;
            this.maxUnavailableNbRetry = maxUnavailableNbRetry;
            this.waitRetryTime = waitRetryTime;
        }

        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses,
                                           int receivedResponses, boolean dataRetrieved, int nbRetry) {
            waitForNextRetry();
            if (dataRetrieved && receivedResponses >= requiredResponses) {
                return RetryDecision.ignore();
            } else if (nbRetry < maxReadNbRetry) {
                return RetryDecision.retry(cl);
            } else {
                return RetryDecision.rethrow();
            }
        }

        @SuppressWarnings({"squid:S2142", "InterruptedException should not be ignored"})
        private void waitForNextRetry() {
            try {
                sleep(waitRetryTime);
            } catch (InterruptedException e) {
                log.error("Sleep interrupted!", e);
            }
        }

        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType,
                                            int requiredAcks, int receivedAcks, int nbRetry) {
            waitForNextRetry();
            return getRetryDecision(cl, requiredAcks, receivedAcks, nbRetry, maxWriteNbRetry);
        }

        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica,
                                           int aliveReplica, int nbRetry) {
            waitForNextRetry();
            return getRetryDecision(cl, requiredReplica, aliveReplica, nbRetry, maxUnavailableNbRetry);
        }

        public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistencyLevel, DriverException e, int i) {
            return RetryDecision.rethrow();
        }

        public void init(Cluster cluster) {

        }

        public void close() {

        }

        private RetryDecision getRetryDecision(ConsistencyLevel cl, int required, int actual, int nbRetry, double maxNbRetry) {
            if (actual >= required) {
                return RetryDecision.ignore();
            } else if (nbRetry < maxNbRetry) {
                return RetryDecision.retry(cl);
            } else {
                return RetryDecision.rethrow();
            }
        }
    }
}