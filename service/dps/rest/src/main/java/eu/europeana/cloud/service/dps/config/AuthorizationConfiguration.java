package eu.europeana.cloud.service.dps.config;

import eu.europeana.aas.acl.CassandraMutableAclService;
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
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
public class AuthorizationConfiguration {

    private final static String JNDI_KEY_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    private final static String JNDI_KEY_CASSANDRA_PORT = "/aas/cassandra/port";
    private final static String JNDI_KEY_CASSANDRA_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    private final static String JNDI_KEY_CASSANDRA_USERNAME = "/aas/cassandra/user";
    private final static String JNDI_KEY_CASSANDRA_PASSWORD = "/aas/cassandra/password";

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
        String hosts = environment.getProperty(JNDI_KEY_CASSANDRA_HOSTS);
        Integer port = environment.getProperty(JNDI_KEY_CASSANDRA_PORT, Integer.class);
        String keyspaceName = environment.getProperty(JNDI_KEY_CASSANDRA_KEYSPACE);
        String userName = environment.getProperty(JNDI_KEY_CASSANDRA_USERNAME);
        String password = environment.getProperty(JNDI_KEY_CASSANDRA_PASSWORD);

        return new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
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

    @Bean
    public AclPermissionCacheOptimizer permissionCacheOptimizer() {
        return new AclPermissionCacheOptimizer(aclService());
    }


    @Bean
    public AclPermissionEvaluator permissionEvaluator() {
        return new AclPermissionEvaluator(aclService());
    }

}
