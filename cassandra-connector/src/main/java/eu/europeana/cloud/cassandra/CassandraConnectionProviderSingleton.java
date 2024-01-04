package eu.europeana.cloud.cassandra;

/**
 * Created by Tarek on 1/3/2017.
 */
public final class CassandraConnectionProviderSingleton {

  private static CassandraConnectionProvider cassandraConnectionProvider;

  private CassandraConnectionProviderSingleton() {

  }

  /**
   * Instantiates the {@link CassandraConnectionProvider} class with the parameters provided in the method call
   *
   * @param hosts IP list of Cassandra nodes in the cluster
   * @param port  port number used by Cassandra cluster
   * @param keyspaceName  name of the keysprace on cluster
   * @param userName  userName used for connection
   * @param password  password used for connection
   * @return instance of the {@link CassandraConnectionProvider}
   */
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

