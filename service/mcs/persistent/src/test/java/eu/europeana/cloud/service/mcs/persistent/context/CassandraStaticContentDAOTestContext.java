package eu.europeana.cloud.service.mcs.persistent.context;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraStaticContentDAO;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.spy;

@Configuration
public class CassandraStaticContentDAOTestContext {

  @Bean
  public CassandraStaticContentDAO cassandraContentDAO() {
    return spy(new CassandraStaticContentDAO(dbService()));
  }

  @Bean
  public CassandraConnectionProvider dbService() {
    return spy(new CassandraConnectionProvider("localhost", CassandraTestInstance.getPort(), "junit_mcs", "", ""));
  }

}
