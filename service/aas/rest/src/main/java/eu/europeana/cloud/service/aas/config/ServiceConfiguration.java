package eu.europeana.cloud.service.aas.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.web.common.LoggingFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.env.Environment;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Configuration
@EnableWebMvc
@ComponentScan("eu.europeana.cloud.service.aas")
@EnableAspectJAutoProxy
public class ServiceConfiguration implements WebMvcConfigurer {

    public static final String JNDI_KEY_AAS_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    public static final String JNDI_KEY_AAS_CASSANDRA_PORT = "/aas/cassandra/port";
    public static final String JNDI_KEY_AAS_CASSANDRA_AUTHENTICATION_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    public static final String JNDI_KEY_AAS_CASSANDRA_USERNAME = "/aas/cassandra/user";
    public static final String JNDI_KEY_AAS_CASSANDRA_PASSWORD = "/aas/cassandra/password";

    private final Environment environment;

    public ServiceConfiguration(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public CassandraConnectionProvider aasCassandraProvider() {
        String hosts = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_HOSTS);
        Integer port = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_PORT, Integer.class);
        String keyspaceName = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_AUTHENTICATION_KEYSPACE);
        String userName = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_USERNAME);
        String password = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_PASSWORD);

        return new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoggingFilter());
    }

    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

    @Bean
    public RetryAspect retryAspect() {
        return new RetryAspect();
    }
}
