package eu.europeana.cloud.cassandra;

/**
 * Created by Tarek on 1/3/2017.
 */
public final class CassandraConnectionProviderSingleton {

  private static CassandraConnectionProvider cassandraConnectionProvider;

  private CassandraConnectionProviderSingleton() {

  }

  public static synchronized CassandraConnectionProvider getCassandraConnectionProvider(String hosts, int port,
      String keyspaceName,
      String userName,
      String password) {
    if (cassandraConnectionProvider == null) {
      cassandraConnectionProvider = new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }
    return cassandraConnectionProvider;
  }
}

