package eu.europeana.cloud.service.uis.config;

import eu.europeana.aas.authorization.CassandraMutableAclService;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.test.CassandraEnvironment;
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

@Configuration
public class AuthorizationTestContext {

    @Bean
    public CassandraConnectionProvider provider() {
        return new CassandraConnectionProvider(
                CassandraEnvironment.HOST, CassandraEnvironment.getPort(),
                "aas_test" + CassandraEnvironment.KEYSPACE_SUFFIX,
                "",
                ""
        );
    }

    @Bean
    public CassandraAclRepository aclRepository(CassandraConnectionProvider provider) {
        return new CassandraAclRepository(provider, true);
    }

    @Bean
    public DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
        return new DefaultPermissionGrantingStrategy(new ConsoleAuditLogger());
    }

    @Bean
    public AclAuthorizationStrategyImpl authorizationStrategy() {
        return new AclAuthorizationStrategyImpl(
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("ROLE_ADMIN")
        );
    }

    @Bean
    public DefaultPermissionFactory permissionFactory() {
        return new DefaultPermissionFactory();
    }

    @Bean
    public CassandraMutableAclService aclService(
            CassandraAclRepository aclRepository,
            DefaultPermissionGrantingStrategy permissionGrantingStrategy,
            AclAuthorizationStrategyImpl authorizationStrategy,
            DefaultPermissionFactory permissionFactory
    ) {
        return new CassandraMutableAclService(
                aclRepository,
                null, // aclCache
                permissionGrantingStrategy,
                authorizationStrategy,
                permissionFactory
        );
    }

    @Bean
    public PermissionEvaluator permissionEvaluator(CassandraMutableAclService aclService) {
        return new AclPermissionEvaluator(aclService);
    }

    @Bean
    public DefaultMethodSecurityExpressionHandler expressionHandler(PermissionEvaluator permissionEvaluator,
                                                                    CassandraMutableAclService aclService) {
        DefaultMethodSecurityExpressionHandler handler = new DefaultMethodSecurityExpressionHandler();
        handler.setPermissionEvaluator(permissionEvaluator);
        handler.setPermissionCacheOptimizer(new AclPermissionCacheOptimizer(aclService));
        return handler;
    }

    @Bean
    public ACLServiceWrapper aclWrapper(CassandraMutableAclService aclService) {
        return new ACLServiceWrapper(aclService);
    }
}
