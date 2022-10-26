package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentProxy;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.service.web.common.LoggingFilter;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.EnumMap;
import java.util.Map;

@Configuration
@EnableWebMvc
@EnableAsync
@PropertySource("classpath:mcs.properties")
@ComponentScan("eu.europeana.cloud.service.mcs.rest")
@EnableAspectJAutoProxy
public class ServiceConfiguration implements WebMvcConfigurer {
    private static final String JNDI_KEY_CASSANDRA_HOSTS = "/mcs/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "/mcs/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "/mcs/cassandra/keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "/mcs/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "/mcs/cassandra/password";

    private static final String JNDI_KEY_S3_PROVIDER = "/mcs/s3/provider";
    private static final String JNDI_KEY_S3_CONTAINER = "/mcs/s3/container";
    private static final String JNDI_KEY_S3_ENDPOINT = "/mcs/s3/endpoint";
    private static final String JNDI_KEY_S3_USER = "/mcs/s3/user";
    private static final String JNDI_KEY_S3_PASSWORD = "/mcs/s3/password";

    private static final String JNDI_KEY_UISURL = "/mcs/uis-url";
    private static final long MAX_UPLOAD_SIZE = (long)128*1024*1024; //128MB

    private Environment environment;

    public ServiceConfiguration(Environment environment){
        this.environment = environment;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoggingFilter());
    }

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setTaskExecutor(asyncExecutor());
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
    public Integer objectStoreSizeThreshold() {
        return 524288;
    }

    @Bean
    public DynamicContentProxy dynamicContentProxy(SimpleS3ConnectionProvider s3ConnectionProvider) {
        Map<Storage, ContentDAO> params = new EnumMap<>(Storage.class);

        params.put(Storage.OBJECT_STORAGE, s3ContentDAO(s3ConnectionProvider));
        params.put(Storage.DATA_BASE, cassandraContentDAO());

        return new DynamicContentProxy(params);
    }

    @Bean
    public ContentDAO cassandraContentDAO() {
        return new CassandraContentDAO();
    }

    @Bean
    public ContentDAO s3ContentDAO(SimpleS3ConnectionProvider s3ConnectionProvider) {
        return new S3ContentDAO(s3ConnectionProvider);
    }

    @Bean
    public SimpleS3ConnectionProvider s3ConnectionProvider() {
        return new SimpleS3ConnectionProvider(
                environment.getProperty(JNDI_KEY_S3_PROVIDER),
                environment.getProperty(JNDI_KEY_S3_CONTAINER),
                environment.getProperty(JNDI_KEY_S3_ENDPOINT),
                environment.getProperty(JNDI_KEY_S3_USER),
                environment.getProperty(JNDI_KEY_S3_PASSWORD));
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

    @Bean
    public RetryAspect retryAspect() {
        return new RetryAspect();
    }

    @Bean
    public AsyncTaskExecutor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(40);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("MCSThreadPool-");
        return executor;
    }

    @Bean
    public DataSetPermissionsVerifier dataSetPermissionsVerifier(DataSetService dataSetService, PermissionEvaluator permissionEvaluator){
        return new DataSetPermissionsVerifier(dataSetService, permissionEvaluator);
    }
}
