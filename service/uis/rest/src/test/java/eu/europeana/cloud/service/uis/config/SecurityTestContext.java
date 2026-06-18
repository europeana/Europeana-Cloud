package eu.europeana.cloud.service.uis.config;

import eu.europeana.aas.authorization.CassandraMutableAclService;
import eu.europeana.aas.authorization.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.ApplicationContextUtils;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.metric.UisMetricService;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.DataProvidersResource;
import eu.europeana.cloud.service.uis.rest.UniqueIdentifierResource;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
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
public class SecurityTestContext {
    @Bean
    public ApplicationContextUtils applicationContextUtils() {
        return new ApplicationContextUtils();
    }

    @Bean
    public DataProviderService dataProviderService() {
        return Mockito.mock(DataProviderService.class);
    }

    @Bean
    public UniqueIdentifierService uniqueIdentifierService() {
        return Mockito.mock(UniqueIdentifierService.class);
    }

    @Bean
    public DataProviderResource dataProvider(UniqueIdentifierService uniqueIdentifierService, DataProviderService dataProviderService) {
        return new DataProviderResource(uniqueIdentifierService, dataProviderService);
    }

    @Bean
    public DataProvidersResource dataProviders(DataProviderService dataProviderService) {
        return new DataProvidersResource(dataProviderService);
    }

    @Bean
    public UniqueIdentifierResource uniqueIdResource(UniqueIdentifierService uniqueIdentifierService, UisMetricService uisMetricService) {
        return new UniqueIdentifierResource(uniqueIdentifierService, uisMetricService);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
        PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("numberOfElementsOnPage", "100");
        configurer.setProperties(props);
        return configurer;
    }

    @Bean
    public CassandraConnectionProvider provider() {
        return new CassandraConnectionProvider(
                "localhost",
                eu.europeana.cloud.test.CassandraTestInstance.getPort(),
                "aas_test",
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
