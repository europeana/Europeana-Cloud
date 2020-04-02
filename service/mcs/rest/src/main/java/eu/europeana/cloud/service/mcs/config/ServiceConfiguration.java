package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.mock_impl.AlwaysSuccessfulUISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.aspects.ServiceExceptionTranslator;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
@PropertySource("classpath:mcs.properties")
@ComponentScan("eu.europeana.cloud.service.mcs.rest")
//@EnableAsync
//@AspectJAutoProxy  !! EXPLAIN ???
public class ServiceConfiguration {
    private final static  String JNDI_KEY_CASSANDRA_HOSTS = "/mcs/cassandra/hosts";
    private final static  String JNDI_KEY_CASSANDRA_PORT = "/mcs/cassandra/port";
    private final static  String JNDI_KEY_CASSANDRA_KEYSPACE = "/mcs/cassandra/keyspace";
    private final static  String JNDI_KEY_CASSANDRA_USERNAME = "/mcs/cassandra/user";
    private final static  String JNDI_KEY_CASSANDRA_PASSWORD = "/mcs/cassandra/password";

    private final static  String JNDI_KEY_SWIFT_PROVIDER = "/mcs/swift/provider";
    private final static  String JNDI_KEY_SWIFT_CONTAINER = "/mcs/swift/container";
    private final static  String JNDI_KEY_SWIFT_ENDPOINTLIST = "/mcs/swift/endpointList";
    private final static  String JNDI_KEY_SWIFT_USER = "/mcs/swift/user";
    private final static  String JNDI_KEY_SWIFT_PASSWORD = "/mcs/swift/password";

    private Environment environment;

    public ServiceConfiguration(Environment environment){
        this.environment = environment;
    }

    @Bean
    public ServiceExceptionTranslator serviceExceptionTranslator() {
        return new ServiceExceptionTranslator();
    }

    @Bean
    public CassandraConnectionProvider dbService() {
        return new CassandraConnectionProvider(
                environment.getProperty(JNDI_KEY_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_CASSANDRA_KEYSPACE),
                environment.getProperty(JNDI_KEY_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_CASSANDRA_PASSWORD));
    }

    @Bean
    public CassandraDataSetService cassandraDataSetService() {
        return new CassandraDataSetService();
    }

    @Bean
    public CassandraDataSetDAO cassandraDataSetDAO() {
        return new CassandraDataSetDAO();
    }

    @Bean
    public CassandraRecordService cassandraRecordService() {
        return new CassandraRecordService();
    }
     @Bean
    public CassandraRecordDAO cassandraRecordDAO() {
        return new CassandraRecordDAO();
    }

    @Bean
    public SwiftContentDAO swiftContentDAO() {
        return new SwiftContentDAO();
    }

    @Bean
    public SimpleSwiftConnectionProvider swiftConnectionProvider() {
        return new SimpleSwiftConnectionProvider(
                environment.getProperty(JNDI_KEY_SWIFT_PROVIDER),
                environment.getProperty(JNDI_KEY_SWIFT_CONTAINER),
                environment.getProperty(JNDI_KEY_SWIFT_ENDPOINTLIST),
                environment.getProperty(JNDI_KEY_SWIFT_USER),
                environment.getProperty(JNDI_KEY_SWIFT_PASSWORD));
    }

    @Bean
    public AlwaysSuccessfulUISClientHandler uisHandler() {
        return new AlwaysSuccessfulUISClientHandler();
    }
}


