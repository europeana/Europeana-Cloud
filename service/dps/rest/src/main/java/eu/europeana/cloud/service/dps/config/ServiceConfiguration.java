package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.services.DatasetCleanerService;
import eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.kafka.TaskKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraKillService;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraReportService;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraValidationStatisticsService;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
@PropertySource("classpath:dps.properties")
@ComponentScan("eu.europeana.cloud.service.dps.rest")
@EnableAsync
public class ServiceConfiguration {
    private static final String JNDI_KEY_KAFKA_BROKER = "/dps/kafka/brokerLocation";
    private static final String JNDI_KEY_KAFKA_GROUP_ID = "/dps/kafka/groupId";
    private static final String JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS = "/dps/zookeeper/address";

    private static final String JNDI_KEY_CASSANDRA_HOSTS = "/dps/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "/dps/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "/dps/cassandra/keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "/dps/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "/dps/cassandra/password";

    private static final String JNDI_KEY_TOPOLOGY_NAMELIST = "/dps/topology/nameList";
    private static final String JNDI_KEY_TOPOLOGY_AVAILABLETOPICS = "/dps/topology/availableTopics";
    private static final String JNDI_KEY_MCS_LOCATION = "/dps/mcsLocation";
    private static final String JNDI_KEY_APPLICATION_ID = "/dps/appId";

    /** Default logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceConfiguration.class);

    private Environment environment;

    public ServiceConfiguration(Environment environment){
        this.environment = environment;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor();
    }

    @Bean
    public PermissionManager permissionManager() {
        return new PermissionManager();
    }

    @Bean
    @Scope("prototype")
    public FilesCounterFactory filesCounterFactory() {
        return new FilesCounterFactory();
    }

    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

    @Bean
    public TaskKafkaSubmitService taskSubmitService() {
        return new TaskKafkaSubmitService(
                environment.getProperty(JNDI_KEY_KAFKA_BROKER),
                environment.getProperty(JNDI_KEY_KAFKA_GROUP_ID),
                environment.getProperty(JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS));
    }

    @Bean
    public RecordKafkaSubmitService recordSubmitService() {
        return new RecordKafkaSubmitService(
                environment.getProperty(JNDI_KEY_KAFKA_BROKER),
                environment.getProperty(JNDI_KEY_KAFKA_GROUP_ID),
                environment.getProperty(JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS));
    }

    @Bean
    public CassandraReportService taskReportService() {
        return new CassandraReportService(
                environment.getProperty(JNDI_KEY_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_CASSANDRA_KEYSPACE),
                environment.getProperty(JNDI_KEY_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_CASSANDRA_PASSWORD));
    }

    @Bean
    public TopologyManager topologyManger() {
        return new TopologyManager(environment.getProperty(JNDI_KEY_TOPOLOGY_NAMELIST));
    }

    @Bean
    public RecordServiceClient recordServiceClient() {
        return new RecordServiceClient(environment.getProperty(JNDI_KEY_MCS_LOCATION));
    }

    @Bean
    public FileServiceClient fileServiceClient() {
        return new FileServiceClient(environment.getProperty(JNDI_KEY_MCS_LOCATION));
    }

    @Bean
    public DataSetServiceClient dataSetServiceClient() {
        return new DataSetServiceClient(environment.getProperty(JNDI_KEY_MCS_LOCATION));
    }

    @Bean
    public CassandraConnectionProvider dpsCassandraProvider() {
        return new CassandraConnectionProvider(
                environment.getProperty(JNDI_KEY_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_CASSANDRA_KEYSPACE),
                environment.getProperty(JNDI_KEY_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_CASSANDRA_PASSWORD));
    }

    @Bean
    public String applicationIdentifier() {
        return environment.getProperty(JNDI_KEY_APPLICATION_ID);
    }

    @Bean
    public KafkaTopicSelector kafkaTopicSelector() {
        String topologiesTopics = environment.getProperty(JNDI_KEY_TOPOLOGY_AVAILABLETOPICS);
        return new KafkaTopicSelector(topologiesTopics);
    }

    //INSERT MethodInvokingFactoryBean here

    @Bean
    public MethodInvokingFactoryBean methodInvokingFactoryBean() {
        MethodInvokingFactoryBean result = new MethodInvokingFactoryBean();
        result.setTargetClass(SecurityContextHolder.class);
        result.setTargetMethod("setStrategyName");
        result.setArguments("MODE_INHERITABLETHREADLOCAL");
        return result;
    }

    @Bean
    public CassandraTaskInfoDAO taskInfoDAO() {
        return new CassandraTaskInfoDAO(dpsCassandraProvider());
    }

    @Bean
    public TasksByStateDAO tasksByStateDAO() {
        return new TasksByStateDAO(dpsCassandraProvider());
    }

    @Bean
    public CassandraKillService killService() {
        return new CassandraKillService(dpsCassandraProvider());
    }

    @Bean
    public CassandraValidationStatisticsService validationStatisticsService() {
        return new CassandraValidationStatisticsService();
    }

    @Bean
    public CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO() {
        return new CassandraNodeStatisticsDAO(dpsCassandraProvider());
    }

    @Bean
    public CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO() {
        return new CassandraAttributeStatisticsDAO(dpsCassandraProvider());
    }

    @Bean
    public TaskStatusChecker taskStatusChecker() {
        return new TaskStatusChecker(taskInfoDAO());
    }

    @Bean
    public CassandraSubTaskInfoDAO subTaskInfoDAO() {
        return new CassandraSubTaskInfoDAO(dpsCassandraProvider());
    }

    @Bean
    public ProcessedRecordsDAO processedRecordsDAO() {
        return new ProcessedRecordsDAO(dpsCassandraProvider());
    }

    @Bean
    public DatasetCleanerService datasetCleanerService(){
        return new DatasetCleanerService(taskInfoDAO());
    }

}
