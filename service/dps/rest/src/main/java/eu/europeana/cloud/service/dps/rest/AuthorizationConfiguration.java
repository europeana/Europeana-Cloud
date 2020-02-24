package eu.europeana.cloud.service.dps.rest;

import eu.europeana.aas.acl.CassandraMutableAclService;
import eu.europeana.aas.acl.repository.AclRepository;
import eu.europeana.aas.acl.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.acls.AclPermissionCacheOptimizer;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.*;
import org.springframework.security.acls.model.AclCache;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
public class AuthorizationConfiguration {

    private static final String JNDI_KEY_CASSANDRA_HOSTS = "java:comp/env/aas/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "java:comp/env/aas/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "java:comp/env/aas/cassandra/authentication-keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "java:comp/env/aas/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "java:comp/env/aas/cassandra/password";



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
    public CassandraMutableAclService aclService() {
        return new CassandraMutableAclService(
                aclRepository(),
                null,
                permissionGrantingStrategy(),
                authorizationStrategy(),
                permissionFactory());
    }

    @Bean
    public CassandraAclRepository aclRepository() {
        return new CassandraAclRepository(cassandraProvider(), false);
    }

    @Bean
    public CassandraConnectionProvider cassandraProvider() {
        String hosts = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_HOSTS);
        Integer port = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_PORT, Integer.class);
        String keyspaceName = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_KEYSPACE);
        String userName = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_USERNAME);
        String password = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_PASSWORD);

        return new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    //@Bean
    private ConsoleAuditLogger auditLogger() {
        return new ConsoleAuditLogger();
    }


    @Bean
    public DefaultPermissionGrantingStrategy permissionGrantingStrategy() {
        return new DefaultPermissionGrantingStrategy(auditLogger());
    }

    //@Bean
    private SimpleGrantedAuthority simpleGrantedAuthority() {
        return new SimpleGrantedAuthority("ROLE_ADMIN");
    }


    @Bean
    public AclAuthorizationStrategyImpl authorizationStrategy() {
        return new AclAuthorizationStrategyImpl(
                simpleGrantedAuthority(),
                simpleGrantedAuthority(),
                simpleGrantedAuthority()
        );
    }

    @Bean
    public DefaultPermissionFactory permissionFactory() {
        return new DefaultPermissionFactory();
    }


    @Bean
    public DefaultMethodSecurityExpressionHandler expressionHandler() {
        DefaultMethodSecurityExpressionHandler result = new DefaultMethodSecurityExpressionHandler();

        result.setPermissionEvaluator(permissionEvaluator());
        result.setPermissionCacheOptimizer(permissionCacheOptimizer());

        return result;
    }

    //@Bean
    public AclPermissionCacheOptimizer permissionCacheOptimizer() {
        return new AclPermissionCacheOptimizer(aclService());
    }


    @Bean
    public AclPermissionEvaluator permissionEvaluator() {
        return new AclPermissionEvaluator(aclService());
    }

}
