package eu.europeana.cloud.service.uis.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.service.CassandraDataProviderService;
import eu.europeana.cloud.service.uis.service.CassandraUniqueIdentifierService;
import eu.europeana.cloud.service.uis.dao.CassandraCloudIdDAO;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.CassandraLocalIdDAO;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;


@Configuration
@EnableWebMvc
@ComponentScan("eu.europeana.cloud.service.uis")
public class ServiceConfiguration {

    public static final String JNDI_KEY_AAS_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    public static final String JNDI_KEY_AAS_CASSANDRA_PORT = "/aas/cassandra/port";
    public static final String JNDI_KEY_AAS_CASSANDRA_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    public static final String JNDI_KEY_AAS_CASSANDRA_USERNAME = "/aas/cassandra/user";
    public static final String JNDI_KEY_AAS_CASSANDRA_PASSWORD = "/aas/cassandra/password";

    public static final String JNDI_KEY_UIS_CASSANDRA_HOSTS = "/uis/cassandra/hosts";
    public static final String JNDI_KEY_UIS_CASSANDRA_PORT = "/uis/cassandra/port";
    public static final String JNDI_KEY_UIS_CASSANDRA_KEYSPACE = "/uis/cassandra/keyspace";
    public static final String JNDI_KEY_UIS_CASSANDRA_USERNAME = "/uis/cassandra/user";
    public static final String JNDI_KEY_UIS_CASSANDRA_PASSWORD = "/uis/cassandra/password";

    private final Environment environment;

    public ServiceConfiguration(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public UniqueIdentifierService uniqueIdentifierService(
            CassandraCloudIdDAO cassandraCloudIdDAO,
            CassandraLocalIdDAO cassandraLocalIdDAO,
            CassandraDataProviderDAO cassandraDataProviderDAO) {
        return new CassandraUniqueIdentifierService(
                cassandraCloudIdDAO,
                cassandraLocalIdDAO,
                cassandraDataProviderDAO);
    }

    @Bean
    public CassandraCloudIdDAO cassandraCloudIdDAO(CassandraConnectionProvider dataProviderDao){
        return new CassandraCloudIdDAO(dataProviderDao);
    }

    @Bean
    public CassandraLocalIdDAO cassandraLocalIdDAO(CassandraConnectionProvider dataProviderDao) {
        return new CassandraLocalIdDAO(dataProviderDao);
    }

    @Bean
    public CassandraDataProviderService cassandraDataProviderService(CassandraDataProviderDAO dataProviderDAO) {
        return new CassandraDataProviderService(dataProviderDAO);
    }

    @Bean
    public CassandraDataProviderDAO cassandraDataProviderDAO(CassandraConnectionProvider dataProviderDao) {
        return new CassandraDataProviderDAO(dataProviderDao);
    }

    @Bean
    public CassandraConnectionProvider dataProviderDao() {
        return new CassandraConnectionProvider(
                environment.getProperty(JNDI_KEY_UIS_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_UIS_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_UIS_CASSANDRA_KEYSPACE),
                environment.getProperty(JNDI_KEY_UIS_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_UIS_CASSANDRA_PASSWORD));
    }

    @Bean
    public CassandraConnectionProvider aasCassandraProvider() {
        String hosts = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_HOSTS);
        Integer port = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_PORT, Integer.class);
        String keyspaceName = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_KEYSPACE);
        String userName = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_USERNAME);
        String password = environment.getProperty(JNDI_KEY_AAS_CASSANDRA_PASSWORD);

        return new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    @Bean
    public BucketsHandler bucketsHandler() {
        return new BucketsHandler(dataProviderDao().getSession());
    }

    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
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
