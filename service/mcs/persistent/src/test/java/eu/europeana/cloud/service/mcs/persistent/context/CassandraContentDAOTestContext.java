package eu.europeana.cloud.service.mcs.persistent.context;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.spy;

@Configuration
public class CassandraContentDAOTestContext {

  @Bean
  public CassandraContentDAO cassandraContentDAO() {
    return spy(new CassandraContentDAO(dbService()));
  }

  @Bean
  public CassandraConnectionProvider dbService() {
    return spy(new CassandraConnectionProvider(CassandraEnvironment.HOST, CassandraEnvironment.getPort(),
            "junit_mcs" + CassandraEnvironment.KEYSPACE_SUFFIX, "", ""));
  }

}
