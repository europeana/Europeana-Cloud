package eu.europeana.cloud.service.mcs.config;

import eu.europeana.aas.authorization.CassandraMutableAclService;
import eu.europeana.aas.authorization.ExtendedAclService;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.aas.permission.PermissionsGrantingManager;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.Role;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.common.properties.CassandraProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.acls.AclPermissionCacheOptimizer;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.AclAuthorizationStrategyImpl;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
public class AuthorizationConfiguration {

  @Bean
  CassandraConnectionProvider aasCassandraProvider(
      @Qualifier("aasProperties") CassandraProperties cassandraAASProperties) {
    return new CassandraConnectionProvider(
        cassandraAASProperties.getHosts(),
        cassandraAASProperties.getPort(),
        cassandraAASProperties.getKeyspace(),
        cassandraAASProperties.getUser(),
        cassandraAASProperties.getPassword());
  }

  /* Custom success handler, answers requests with 200 OK. */
  @Bean
  CloudAuthenticationSuccessHandler cloudSecuritySuccessHandler() {
    return new CloudAuthenticationSuccessHandler();
  }


  /* Custom failure handler, answers requests with 401. */
  @Bean
  SimpleUrlAuthenticationFailureHandler cloudSecurityFailureHandler() {
    return new SimpleUrlAuthenticationFailureHandler();
  }


  /* ========= PERMISSION STORAGE in CASSANDRA (Using Spring security ACL) ========= */

  @Bean
  ExtendedAclService aclService(CassandraAclRepository aclRepository) {
    return new CassandraMutableAclService(
        aclRepository,
        null,
        permissionGrantingStrategy(),
        authorizationStrategy(),
        permissionFactory());
  }

  @Bean
  CassandraAclRepository aclRepository(CassandraConnectionProvider aasCassandraProvider) {
    return new CassandraAclRepository(aasCassandraProvider, false);
  }

  @Bean("aasProperties")
  @ConfigurationProperties(prefix = "cassandra.aas")
  CassandraProperties cassandraAASProperties() {
    return new CassandraProperties();
  }

  @Bean
  DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
    return new DefaultPermissionGrantingStrategy(auditLogger());
  }

  @Bean
  AclAuthorizationStrategyImpl authorizationStrategy() {
    return new AclAuthorizationStrategyImpl(simpleGrantedAuthority());
  }


  @Bean
  DefaultPermissionFactory permissionFactory() {
    return new DefaultPermissionFactory();
  }


  @Bean
  ConsoleAuditLogger auditLogger() {
    return new ConsoleAuditLogger();
  }

  @Bean
  SimpleGrantedAuthority simpleGrantedAuthority() {
    return new SimpleGrantedAuthority(Role.ADMIN);
  }

  @Bean
  PermissionsGrantingManager permissionsGrantingManager() {
    return new PermissionsGrantingManager();
  }

  @Bean
  DefaultMethodSecurityExpressionHandler expressionHandler(AclPermissionEvaluator permissionEvaluator,
      AclPermissionCacheOptimizer permissionCacheOptimizer) {
    DefaultMethodSecurityExpressionHandler result = new DefaultMethodSecurityExpressionHandler();

    result.setPermissionEvaluator(permissionEvaluator);
    result.setPermissionCacheOptimizer(permissionCacheOptimizer);

    return result;
  }

  @Bean
  AclPermissionCacheOptimizer permissionCacheOptimizer(ExtendedAclService aclService) {
    return new AclPermissionCacheOptimizer(aclService);
  }


  @Bean
  AclPermissionEvaluator permissionEvaluator(ExtendedAclService aclService) {
    return new AclPermissionEvaluator(aclService);
  }

  @Bean
  DataSetPermissionsVerifier dataSetPermissionsVerifier(DataSetService dataSetService,
      PermissionEvaluator permissionEvaluator) {
    return new DataSetPermissionsVerifier(dataSetService, permissionEvaluator);
  }
}
