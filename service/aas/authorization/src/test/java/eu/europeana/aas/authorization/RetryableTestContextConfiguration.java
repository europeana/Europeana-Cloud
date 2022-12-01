package eu.europeana.aas.authorization;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.datastax.driver.core.Session;
import eu.europeana.aas.authorization.repository.AclRepository;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy
public class RetryableTestContextConfiguration {

  @Bean
  public Session session() {
    return mock(Session.class);
  }

  @Bean
  public String keyspace() {
    return "test";
  }

  @Bean
  public RetryAspect retryAspect() {
    return spy(RetryAspect.class);
  }

  @Bean
  public AclRepository cassandraAclRepository(Session session, String keyspace) {
    return new CassandraAclRepository(session, keyspace);
  }

}