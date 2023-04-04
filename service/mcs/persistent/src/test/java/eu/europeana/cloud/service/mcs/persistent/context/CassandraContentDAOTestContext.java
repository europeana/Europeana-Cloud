package eu.europeana.cloud.service.mcs.persistent.context;

import static org.mockito.Mockito.spy;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class CassandraContentDAOTestContext {

  @Bean
  public CassandraContentDAO cassandraContentDAO() {
    return spy(new CassandraContentDAO(dbService()));
  }

  @Bean
  public CassandraConnectionProvider dbService() {
    return spy(new CassandraConnectionProvider("localhost", CassandraTestInstance.getPort(), "junit_mcs", "", ""));
  }

}
