package eu.europeana.aas.permission.config;

import eu.europeana.aas.authorization.CassandraMutableAclService;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.aas.permission.PermissionsGrantingManager;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.acls.domain.*;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

@Configuration
public class DefaultTestContext {
    @Bean
    public CassandraConnectionProvider provider() {
        return new CassandraConnectionProvider(
                CassandraEnvironment.HOST, CassandraEnvironment.getPort(),
                "test_keyspace" + CassandraEnvironment.KEYSPACE_SUFFIX,
                "",
                ""
        );
    }


    @Bean
    public CassandraAclRepository aclRepository(
            CassandraConnectionProvider provider) {

        return new CassandraAclRepository(
                provider.getSession(),
                provider.getKeyspaceName(),
                true
        );
    }


    @Bean
    public ConsoleAuditLogger auditLogger() {
        return new ConsoleAuditLogger();
    }

    @Bean
    public PermissionGrantingStrategy permissionGrantingStrategy(ConsoleAuditLogger auditLogger) {
        return new DefaultPermissionGrantingStrategy(auditLogger);
    }

    @Bean
    public PermissionFactory permissionFactory() {
        return new DefaultPermissionFactory();
    }

    @Bean
    public AclAuthorizationStrategy authorizationStrategy() {
        return new AclAuthorizationStrategyImpl(
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("ROLE_ADMIN")
        );
    }


    @Bean
    public CassandraMutableAclService mutableAclService(
            CassandraAclRepository aclRepository,
            PermissionGrantingStrategy permissionGrantingStrategy,
            AclAuthorizationStrategy authorizationStrategy,
            PermissionFactory permissionFactory) {

        return new CassandraMutableAclService(
                aclRepository,
                null,
                permissionGrantingStrategy,
                authorizationStrategy,
                permissionFactory
        );
    }


    @Bean
    public PermissionsGrantingManager permissionsGrantingManager() {
        return new PermissionsGrantingManager();
    }
}
