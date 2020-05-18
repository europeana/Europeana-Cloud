package eu.europeana.cloud.service.mcs.config;

import eu.europeana.aas.acl.CassandraMutableAclService;
import eu.europeana.aas.acl.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import eu.europeana.cloud.service.commons.permissions.PermissionsGrantingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.acls.AclPermissionCacheOptimizer;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.AclAuthorizationStrategyImpl;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

import java.util.*;

@Configuration
public class AuthorizationConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationConfiguration.class);

    private static final String JNDI_KEY_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "/aas/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "/aas/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "/aas/cassandra/password";

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
    public CassandraMutableAclService aclService(CassandraAclRepository aclRepository) {
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
    public AclAuthorizationStrategyImpl authorizationStrategy() {
        return new AclAuthorizationStrategyImpl(simpleGrantedAuthority());
    }


    @Bean
    public DefaultPermissionFactory permissionFactory() {
        return new DefaultPermissionFactory();
    }

    @Bean
    public CassandraConnectionProvider aasCassandraProvider() {
        listEnvironment();

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
    public SimpleGrantedAuthority simpleGrantedAuthority() {
        return new SimpleGrantedAuthority("ROLE_ADMIN");
    }

    @Bean
    public PermissionsGrantingManager permissionsGrantingManager() {
        return new PermissionsGrantingManager();
    }

    @Bean
    public DefaultMethodSecurityExpressionHandler expressionHandler(AclPermissionEvaluator permissionEvaluator, AclPermissionCacheOptimizer permissionCacheOptimizer) {
        DefaultMethodSecurityExpressionHandler result = new DefaultMethodSecurityExpressionHandler();

        result.setPermissionEvaluator(permissionEvaluator);
        result.setPermissionCacheOptimizer(permissionCacheOptimizer);

        return result;
    }

    @Bean
    public AclPermissionCacheOptimizer permissionCacheOptimizer(CassandraMutableAclService aclService) {
        return new AclPermissionCacheOptimizer(aclService);
    }


    @Bean
    public AclPermissionEvaluator permissionEvaluator(CassandraMutableAclService aclService) {
        return new AclPermissionEvaluator(aclService);
    }

    private void listEnvironment() {
        Map<String, Object> map = new TreeMap<>();
        for(Iterator it = ((AbstractEnvironment)environment).getPropertySources().iterator(); it.hasNext(); ) {
            PropertySource propertySource = (PropertySource) it.next();
            if (propertySource instanceof MapPropertySource) {
                map.putAll(((MapPropertySource) propertySource).getSource());
            }
        }

        Iterator<String> keyIterator = map.keySet().iterator();

        LOGGER.info("###MAP: ");
        while(keyIterator.hasNext()) {
            String key = keyIterator.next();
            Object value = map.get(key);
            LOGGER.info("\t{}: {}", key, value);
        }
    }
}
