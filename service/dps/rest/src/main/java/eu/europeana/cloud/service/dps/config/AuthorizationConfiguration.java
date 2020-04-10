package eu.europeana.cloud.service.dps.config;

import eu.europeana.aas.acl.CassandraMutableAclService;
import eu.europeana.aas.acl.repository.AclRepository;
import eu.europeana.aas.acl.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.acls.AclPermissionCacheOptimizer;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.AclAuthorizationStrategyImpl;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.acls.model.AclService;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
public class AuthorizationConfiguration {

    @Autowired
    private Environment environment;

    /* Ecloud persistent authorization application context. Permissions are stored in cassandra. */

    /* Custom success handler, answers requests with 200 OK. */
    @Bean
    public CloudAuthenticationSuccessHandler cloudSecuritySuccessHandler() {
        return new CloudAuthenticationSuccessHandler();
    }


    /* Custom failure handler, answers requests with 401. */
    @Bean
    public SimpleUrlAuthenticationFailureHandler cloudSecurityFailureHandler() {
        return new SimpleUrlAuthenticationFailureHandler();
    }


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
    public ConsoleAuditLogger auditLogger() {
        return new ConsoleAuditLogger();
    }


    @Bean
    public DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
        return new DefaultPermissionGrantingStrategy(auditLogger());
    }

    public SimpleGrantedAuthority simpleGrantedAuthority() {
        return new SimpleGrantedAuthority("ROLE_ADMIN");
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
    public DefaultMethodSecurityExpressionHandler expressionHandler(AclPermissionEvaluator permissionEvaluator,AclPermissionCacheOptimizer permissionCacheOptimizer) {
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

}
