package eu.europeana.cloud.service.aas.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;


@Configuration
@EnableWebMvc
@ComponentScan("eu.europeana.cloud.service.aas")
public class ServiceConfiguration {

    public static final String JNDI_KEY_AAS_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    public static final String JNDI_KEY_AAS_CASSANDRA_PORT = "/aas/cassandra/port";
    public static final String JNDI_KEY_AAS_CASSANDRA_AUTHENTICATION_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    public static final String JNDI_KEY_AAS_CASSANDRA_AUTHORIZATION_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    public static final String JNDI_KEY_AAS_CASSANDRA_USERNAME = "/aas/cassandra/user";
    public static final String JNDI_KEY_AAS_CASSANDRA_PASSWORD = "/aas/cassandra/password";

    private final Environment environment;

    public ServiceConfiguration(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public CassandraConnectionProvider dataProviderDao() {
        return new CassandraConnectionProvider(
                environment.getProperty(JNDI_KEY_AAS_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_AAS_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_AAS_CASSANDRA_AUTHORIZATION_KEYSPACE),
                environment.getProperty(JNDI_KEY_AAS_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_AAS_CASSANDRA_PASSWORD));
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

    /////////////////////
    //
    /////////////////////

    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

}
