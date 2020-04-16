package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentDAO;
import eu.europeana.cloud.service.mcs.persistent.aspects.ServiceExceptionTranslator;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.MarshallingHttpMessageConverter;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.xml.stream.XMLStreamConstants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableWebMvc
@PropertySource("classpath:mcs.properties")
@ComponentScan("eu.europeana.cloud.service.mcs.rest")
//@EnableAsync
//@AspectJAutoProxy  !! EXPLAIN ???
public class ServiceConfiguration implements WebMvcConfigurer {
    private static final String JNDI_KEY_CASSANDRA_HOSTS = "/mcs/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "/mcs/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "/mcs/cassandra/keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "/mcs/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "/mcs/cassandra/password";

    private static final String JNDI_KEY_SWIFT_PROVIDER = "/mcs/swift/provider";
    private static final String JNDI_KEY_SWIFT_CONTAINER = "/mcs/swift/container";
    private static final String JNDI_KEY_SWIFT_ENDPOINT = "/mcs/swift/endpoint";
    private static final String JNDI_KEY_SWIFT_USER = "/mcs/swift/user";
    private static final String JNDI_KEY_SWIFT_PASSWORD = "/mcs/swift/password";

    private static final String JNDI_KEY_UISURL = "/mcs/uis-url";
    private static final long MAX_UPLOAD_SIZE = 128*1024*1024;

    private Environment environment;

    public ServiceConfiguration(Environment environment){
        this.environment = environment;
    }


    @Override
    public void configureMessageConverters(
            List<HttpMessageConverter<?>> converters) {

        converters.add(createXmlHttpMessageConverter());
        converters.add(new MappingJackson2HttpMessageConverter());
    }

    private HttpMessageConverter<Object> createXmlHttpMessageConverter() {
        MarshallingHttpMessageConverter xmlConverter =
                new MarshallingHttpMessageConverter();

        XStreamMarshaller xstreamMarshaller = new XStreamMarshaller();
        xmlConverter.setMarshaller(xstreamMarshaller);
        xmlConverter.setUnmarshaller(xstreamMarshaller);

        return xmlConverter;
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
    public CassandraContentDAO objectStoreSizeThreshold() {
        return new CassandraContentDAO();
    }

    @Bean
    public DynamicContentDAO dynamicContentDAO() {
        Map<Storage, ContentDAO> params = new HashMap<>();

        params.put(Storage.OBJECT_STORAGE, swiftContentDAO());
        params.put(Storage.DATA_BASE, cassandraContentDAO());

        return new DynamicContentDAO(params);
    }

    @Bean
    public CassandraContentDAO cassandraContentDAO() {
        return new CassandraContentDAO();
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
                environment.getProperty(JNDI_KEY_SWIFT_ENDPOINT),
                environment.getProperty(JNDI_KEY_SWIFT_USER),
                environment.getProperty(JNDI_KEY_SWIFT_PASSWORD));
    }

    @Bean
    public UISClientHandler uisHandler() {
        return new UISClientHandlerImpl();
    }

    @Bean
    public UISClient uisClient() {
        return new UISClient(environment.getProperty(JNDI_KEY_UISURL));
    }

    @Bean
    public BucketsHandler bucketsHandler() {
        return new BucketsHandler(dbService().getSession());
    }

    @Bean
    public CommonsMultipartResolver multipartResolver() {
        CommonsMultipartResolver multipartResolver = new CommonsMultipartResolver();
        multipartResolver.setMaxUploadSize(MAX_UPLOAD_SIZE);
        return multipartResolver;
    }
}
