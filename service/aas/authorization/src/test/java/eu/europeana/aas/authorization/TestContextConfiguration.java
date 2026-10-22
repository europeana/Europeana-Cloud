package eu.europeana.aas.authorization;

import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.Role;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.acls.domain.AclAuthorizationStrategyImpl;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;


@Configuration
public class TestContextConfiguration {


  @Bean
  public CassandraConnectionProvider cassandraConnectionProvider() {
    return new CassandraConnectionProvider(CassandraEnvironment.HOST, CassandraEnvironment.getPort(), "aas_test" + CassandraEnvironment.KEYSPACE_SUFFIX, "", "");
  }

  @Bean
  public CassandraMutableAclService mutableAclService() {
    return new CassandraMutableAclService(cassandraAclRepository(),
        null,
        permissionGrantingStrategy(),
        authorizationStrategy(),
        permissionFactory());
  }

  @Bean
  public CassandraAclRepository cassandraAclRepository() {
    return new CassandraAclRepository(cassandraConnectionProvider(), true);
  }

  @Bean
  public DefaultPermissionFactory permissionFactory() {
    return new DefaultPermissionFactory();
  }

  @Bean
  public DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
    return new DefaultPermissionGrantingStrategy(new ConsoleAuditLogger());
  }


  @Bean
  public AclAuthorizationStrategyImpl authorizationStrategy() {
    return new AclAuthorizationStrategyImpl(
        new SimpleGrantedAuthority(Role.ADMIN),
        new SimpleGrantedAuthority(Role.ADMIN),
        new SimpleGrantedAuthority(Role.ADMIN)
    );
  }
}
