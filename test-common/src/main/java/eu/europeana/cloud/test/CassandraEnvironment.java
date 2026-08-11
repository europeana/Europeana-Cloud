package eu.europeana.cloud.test;

import java.util.UUID;

public final class CassandraEnvironment {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9042;
    public static final String KEYSPACE_SUFFIX;

    public static final String HOST;
    public static final int PORT;
    public static final boolean USE_EXTERNAL_CASSANDRA;
    // Because we can use docker Cassandra then our port is mapped to a random port on our machine
    public static int MAPPED_PORT = -1;

    static {
        String host = System.getenv("CASSANDRA_HOST");
        String port = System.getenv("CASSANDRA_PORT");
        String useExternalCassandra = System.getenv("CASSANDRA_EXTERNAL");

        USE_EXTERNAL_CASSANDRA = useExternalCassandra != null && !useExternalCassandra.isBlank() && Boolean.parseBoolean(useExternalCassandra);

        HOST = (host == null || host.isBlank())
                ? DEFAULT_HOST
                : host;

        PORT = (port == null || port.isBlank())
                ? DEFAULT_PORT
                : Integer.parseInt(port);
        if (useExternalCassandra()) {
            KEYSPACE_SUFFIX = "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
        } else {
            KEYSPACE_SUFFIX = "";
        }
    }

    private CassandraEnvironment() {
    }

    public static boolean useExternalCassandra() {
        return USE_EXTERNAL_CASSANDRA;
    }

    public static int getPort() {
        return MAPPED_PORT != -1 ? MAPPED_PORT : PORT;
    }

    public static void setMappedPort(int mappedPort) {
        MAPPED_PORT = mappedPort;
    }
}
