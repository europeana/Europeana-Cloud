package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jndi.JndiTemplate;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.naming.NamingException;

@Configuration
@EnableWebMvc
@RequestMapping
@PropertySource("classpath:dps.properties")
public class ServiceConfiguration {
    private static final String JNDI_KEY_KAFKA_BROKER = "java:comp/env/dps/kafka/brokerLocation";
    private static final String JNDI_KEY_KAFKA_GROUP_ID = "java:comp/env/dps/kafka/groupId";
    private static final String JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS = "java:comp/env/dps/zookeeper/address";

    private static final String JNDI_KEY_CASSANDRA_HOSTS = "java:comp/env/dps/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "java:comp/env/dps/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "java:comp/env/dps/cassandra/keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "java:comp/env/dps/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "java:comp/env/dps/cassandra/password";

    private static final String JNDI_KEY_TOPOLOGY_NAMELIST = "java:comp/env/dps/topology/nameList";
    private static final String JNDI_KEY_TOPOLOGY_AVAILABLETOPICS = "java:comp/env/dps/topology/availableTopics";
    private static final String JNDI_KEY_MCS_LOCATION = "java:comp/env/dps/mcsLocation";
    private static final String JNDI_KEY_APPLICATION_ID = "java:comp/env/dps/appId";

    /** Default logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceConfiguration.class);

    @Bean
    public PropertySourcesPlaceholderConfigurer mappings() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public PermissionManager permissionManager() {
        return new PermissionManager();
    }

    //scope="prototype"
    @Bean
    public FilesCounterFactory filesCounterFactory() {
        return new FilesCounterFactory();
    }

    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

    @Bean
    public TaskKafkaSubmitService taskSubmitService() {
        String kafkaBroker = readJNDIValue(JNDI_KEY_KAFKA_BROKER);
        String kafkaGroupId = readJNDIValue(JNDI_KEY_KAFKA_GROUP_ID);
        String zookeeperAddress = readJNDIValue(JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS);

        return new TaskKafkaSubmitService(kafkaBroker, kafkaGroupId, zookeeperAddress);
    }

    @Bean
    public RecordKafkaSubmitService recordSubmitService() {
        String kafkaBroker = readJNDIValue(JNDI_KEY_KAFKA_BROKER);
        String kafkaGroupId = readJNDIValue(JNDI_KEY_KAFKA_GROUP_ID);
        String zookeeperAddress = readJNDIValue(JNDI_KEY_KAFKA_ZOOKEEPER_ADDRESS);

        return new RecordKafkaSubmitService(kafkaBroker, kafkaGroupId, zookeeperAddress);
    }

    @Bean
    public CassandraReportService taskReportService() {
        String hosts = readJNDIValue(JNDI_KEY_CASSANDRA_HOSTS);
        Integer port = readJNDIValue(JNDI_KEY_CASSANDRA_PORT, Integer.class);
        String keyspaceName = readJNDIValue(JNDI_KEY_CASSANDRA_KEYSPACE);
        String userName = readJNDIValue(JNDI_KEY_CASSANDRA_USERNAME);
        String password = readJNDIValue(JNDI_KEY_CASSANDRA_PASSWORD);

        return new CassandraReportService(hosts, port, keyspaceName, userName, password);
    }

    @Bean
    public TopologyManager topologyManger() {
        String nameList = readJNDIValue(JNDI_KEY_TOPOLOGY_NAMELIST);
        return new TopologyManager(nameList);
    }

    @Bean
    public String mcsLocation() {
        return readJNDIValue(JNDI_KEY_MCS_LOCATION);
    }

    // scope="prototype"
    @Bean
    public RecordServiceClient recordServiceClient() {
        return new RecordServiceClient(mcsLocation());
    }

    // scope="prototype"
    @Bean
    public FileServiceClient fileServiceClient() {
        return new FileServiceClient(mcsLocation());
    }

    // scope="prototype"
    @Bean
    public DataSetServiceClient dataSetServiceClient() {
        return new DataSetServiceClient(mcsLocation());
    }

    @Bean
    public CassandraConnectionProvider dpsCassandraProvider() {
        String hosts = readJNDIValue(JNDI_KEY_CASSANDRA_HOSTS);
        Integer port = readJNDIValue(JNDI_KEY_CASSANDRA_PORT, Integer.class);
        String keyspaceName = readJNDIValue(JNDI_KEY_CASSANDRA_KEYSPACE);
        String userName = readJNDIValue(JNDI_KEY_CASSANDRA_USERNAME);
        String password = readJNDIValue(JNDI_KEY_CASSANDRA_PASSWORD);

        return new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    @Bean
    public String applicationIdentifier() {
        return readJNDIValue(JNDI_KEY_APPLICATION_ID);
    }

    @Bean
    public KafkaTopicSelector kafkaTopicSelector() {
        String topologiesTopics = readJNDIValue(JNDI_KEY_TOPOLOGY_AVAILABLETOPICS);
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

    /*Auxiliary methods*/

    /**
     * Read from server environment String value by JNDI key
     * @param jndiKey Key for value
     * @return Value or null if error occurs
     */
    public static String readJNDIValue(String jndiKey) {
        return readJNDIValue(jndiKey, String.class);
    }

    /**
     * Read from server environment value of given class by JNDI key
     * Write information to log file if no value for given key
     * @param jndiKey Key for value
     * @param clazz Class of returned object
     * @return Value or null if error occurs
     */
    public static <T> T readJNDIValue(String jndiKey, Class<T> clazz) {
        try {
            return new JndiTemplate().lookup(jndiKey, clazz);
        } catch(NamingException ne) {
            LOGGER.warn("Error while reading configuration from server environment for key: '"+jndiKey+"'", ne);
        }
        return null;
    }
}
