package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class TestContextConfiguration {

  @Bean
  public CassandraConnectionProvider cassandraConnectionProvider() {
    return new CassandraConnectionProvider(CassandraEnvironment.HOST, CassandraEnvironment.getPort(), "aas_test" + CassandraEnvironment.KEYSPACE_SUFFIX, "", "");
  }

  @Bean
  public CassandraAuthenticationService cassandraAuthenticationService() {
    return new CassandraAuthenticationService(cassandraUserDAO());
  }

  @Bean
  public CassandraUserDAO cassandraUserDAO() {
    return new CassandraUserDAO(cassandraConnectionProvider());
  }
}
