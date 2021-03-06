package eu.europeana.cloud.service.aas.config;

import eu.europeana.aas.acl.CassandraMutableAclService;
import eu.europeana.aas.acl.repository.AclRepository;
import eu.europeana.aas.acl.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
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
    public CassandraAclRepository aclRepository(CassandraConnectionProvider aasCassandraProvider) {
        return new CassandraAclRepository(aasCassandraProvider, false);
    }

    @Bean
    public DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
        return new DefaultPermissionGrantingStrategy(auditLogger());
    }

    @Bean
    public DefaultPermissionFactory permissionFactory() {
        return new DefaultPermissionFactory();
    }

    @Bean
    public AclAuthorizationStrategyImpl authorizationStrategy() {
        return new AclAuthorizationStrategyImpl(simpleGrantedAuthority());
    }

    public SimpleGrantedAuthority simpleGrantedAuthority() {
        return new SimpleGrantedAuthority("ROLE_ADMIN");
    }

    @Bean
    public DefaultMethodSecurityExpressionHandler expressionHandler(AclPermissionEvaluator permissionEvaluator, AclPermissionCacheOptimizer permissionCacheOptimizer) {
        DefaultMethodSecurityExpressionHandler result = new DefaultMethodSecurityExpressionHandler();

        result.setPermissionEvaluator(permissionEvaluator);
        result.setPermissionCacheOptimizer(permissionCacheOptimizer);

        return result;
    }

    @Bean
    public AclPermissionEvaluator permissionEvaluator(AclService aclService) {
        return new AclPermissionEvaluator(aclService);
    }

    @Bean
    public ConsoleAuditLogger auditLogger() {
        return new ConsoleAuditLogger();
    }

    @Bean
    public AclPermissionCacheOptimizer permissionCacheOptimizer(AclService aclService) {
        return new AclPermissionCacheOptimizer(aclService);
    }

}

