package eu.europeana.cloud.service.dps.config;

import eu.europeana.aas.acl.CassandraMutableAclService;
import eu.europeana.aas.acl.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.acls.AclPermissionCacheOptimizer;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.acls.domain.AclAuthorizationStrategyImpl;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

@Configuration
@Import({AclPermissionEvaluator.class})
public class AuthorizationTestContext {

    @Bean
    TopologyManager topologyManager() {
        return Mockito.mock(TopologyManager.class);
    }

    @Bean
    public MutableAclService aclService(CassandraAclRepository aclRepository, DefaultPermissionGrantingStrategy permissionGrantingStrategy, AclAuthorizationStrategyImpl authorizationStrategy, DefaultPermissionFactory permissionFactory) {
        return new CassandraMutableAclService(
                aclRepository,
                null,
                permissionGrantingStrategy,
                authorizationStrategy,
                permissionFactory);
    }

    @Bean
    public CassandraConnectionProvider provider() {
        return new CassandraConnectionProvider("localhost", 9142, "ecloud_aas_tests", "", "");
    }


    @Bean
    public CassandraAclRepository aclRepository(CassandraConnectionProvider provider) {
        return new CassandraAclRepository(provider, true);
    }


    @Bean
    public DefaultPermissionGrantingStrategy permissionGrantingStrategy(ConsoleAuditLogger auditLogger) {
        return new DefaultPermissionGrantingStrategy(auditLogger);
    }

    @Bean
    public DefaultPermissionFactory permissionFactory() {
        return new DefaultPermissionFactory();
    }

    @Bean
    public AclAuthorizationStrategyImpl authorizationStrategy() {
        return new AclAuthorizationStrategyImpl(
                simpleGrantedAuthority()
        );
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
    public MethodInvokingFactoryBean methodInvokingFactoryBean() {
        MethodInvokingFactoryBean result = new MethodInvokingFactoryBean();
        result.setTargetClass(SecurityContextHolder.class);
        result.setTargetMethod("setStrategyName");
        result.setArguments("MODE_INHERITABLETHREADLOCAL");
        return result;
    }
}
