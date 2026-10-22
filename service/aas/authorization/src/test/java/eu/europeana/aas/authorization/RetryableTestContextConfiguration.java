package eu.europeana.aas.authorization;

import com.datastax.driver.core.Session;
import eu.europeana.aas.authorization.repository.AclRepository;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import static org.mockito.Mockito.mock;

@Configuration
@EnableAspectJAutoProxy
public class RetryableTestContextConfiguration {

  @Bean
  public Session session() {
    return mock(Session.class);
  }

  @Bean
  public String keyspace() {
    return "test" + CassandraEnvironment.KEYSPACE_SUFFIX;
  }

  @Bean
  public RetryAspect retryAspect() {
    return new RetryAspect();
  }

  @Bean
  public AclRepository cassandraAclRepository(Session session, String keyspace) {
    return new CassandraAclRepository(session, keyspace);
  }

}