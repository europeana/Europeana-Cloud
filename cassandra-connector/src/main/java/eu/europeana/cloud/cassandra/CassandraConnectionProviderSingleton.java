package eu.europeana.cloud.cassandra;

/**
 * Created by Tarek on 1/3/2017.
 */
public class CassandraConnectionProviderSingleton {

    private CassandraConnectionProviderSingleton() {

    }

    private static CassandraConnectionProvider cassandraConnectionProvider = null;

    public static CassandraConnectionProvider getCassandraConnectionProvider(String hosts, int port, String keyspaceName, String userName, String password) {
        if (cassandraConnectionProvider == null) {
            synchronized (CassandraConnectionProviderSingleton.class) {
                if (cassandraConnectionProvider == null) {
                    cassandraConnectionProvider = new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
                }
            }
        }
        return cassandraConnectionProvider;
    }

}

