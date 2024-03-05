package eu.europeana.cloud.service.uis.config;

import eu.europeana.aas.authorization.CassandraMutableAclService;
import eu.europeana.aas.authorization.ExtendedAclService;
import eu.europeana.aas.authorization.repository.AclRepository;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.Role;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.acls.AclPermissionCacheOptimizer;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.AclAuthorizationStrategyImpl;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.acls.model.AclService;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

@Configuration
public class AuthorizationConfiguration {

  /* ========= PERMISSION STORAGE in CASSANDRA (Using Spring security ACL) ========= */

  @Bean
  public CassandraMutableAclService aclService(AclRepository aclRepository) {
    return new CassandraMutableAclService(
        aclRepository,
        null,
        permissionGrantingStrategy(),
        authorizationStrategy(),
        permissionFactory());
  }

  @Bean
  public CassandraAclRepository aclRepository(
      @Qualifier("aasCassandraProvider") CassandraConnectionProvider aasCassandraProvider) {
    return new CassandraAclRepository(aasCassandraProvider, false);
  }

  @Bean
  public ConsoleAuditLogger auditLogger() {
    return new ConsoleAuditLogger();
  }

  @Bean
  public DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
    return new DefaultPermissionGrantingStrategy(auditLogger());
  }

  public SimpleGrantedAuthority simpleGrantedAuthority() {
    return new SimpleGrantedAuthority(Role.ADMIN);
  }

  @Bean
  public AclAuthorizationStrategyImpl authorizationStrategy() {
    return new AclAuthorizationStrategyImpl(simpleGrantedAuthority());
  }

  @Bean
  public DefaultPermissionFactory permissionFactory() {
    return new DefaultPermissionFactory();
  }

  @Bean
  public DefaultMethodSecurityExpressionHandler expressionHandler(AclPermissionEvaluator permissionEvaluator,
      AclPermissionCacheOptimizer permissionCacheOptimizer) {
    DefaultMethodSecurityExpressionHandler result = new DefaultMethodSecurityExpressionHandler();

    result.setPermissionEvaluator(permissionEvaluator);
    result.setPermissionCacheOptimizer(permissionCacheOptimizer);

    return result;
  }

  @Bean
  public AclPermissionCacheOptimizer permissionCacheOptimizer(AclService aclService) {
    return new AclPermissionCacheOptimizer(aclService);
  }

  @Bean
  public AclPermissionEvaluator permissionEvaluator(AclService aclService) {
    return new AclPermissionEvaluator(aclService);
  }

  @Bean
  public ACLServiceWrapper aclServiceWrapper(ExtendedAclService extendedAclService) {
    return new ACLServiceWrapper(extendedAclService);
  }

}
