package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.kafka.TaskKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraReportService;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraValidationStatisticsService;
import eu.europeana.cloud.service.dps.services.submitters.MCSTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.*;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import static eu.europeana.cloud.service.dps.config.JndiNames.*;

@Configuration
@EnableWebMvc
@PropertySource("classpath:dps.properties")
@ComponentScan("eu.europeana.cloud.service.dps")
public class ServiceConfiguration {

    private final Environment environment;

    public ServiceConfiguration(Environment environment){
        this.environment = environment;
    }

    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

    @Bean
    public TaskKafkaSubmitService taskKafkaSubmitService() {
        return new TaskKafkaSubmitService(
                environment.getProperty(JNDI_KEY_KAFKA_BROKER));
    }

    @Bean
    public RecordExecutionSubmitService recordKafkaSubmitService() {
        return new RecordKafkaSubmitService(
                environment.getProperty(JNDI_KEY_KAFKA_BROKER));
    }

    @Bean
    public RecordSubmitService recordSubmitService() {
        return new RecordSubmitService(processedRecordsDAO(), recordKafkaSubmitService());
    }

    @Bean
    public CassandraReportService taskReportService() {
        return new CassandraReportService(
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_KEYSPACE),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_PASSWORD));
    }

    @Bean
    public TopologyManager topologyManger() {
        return new TopologyManager(environment.getProperty(JNDI_KEY_TOPOLOGY_NAMELIST));
    }

    @Bean
    public DataSetServiceClient dataSetServiceClient() {
        return new DataSetServiceClient(environment.getProperty(JNDI_KEY_MCS_LOCATION));
    }

    @Bean
    public CassandraConnectionProvider dpsCassandraProvider() {
        return new CassandraConnectionProvider(
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_HOSTS),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_PORT, Integer.class),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_KEYSPACE),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_USERNAME),
                environment.getProperty(JNDI_KEY_DPS_CASSANDRA_PASSWORD));
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
    public String applicationIdentifier() {
        return environment.getProperty(JNDI_KEY_APPLICATION_ID);
    }

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
    public HarvestedRecordsDAO harvestedRecordsDAO() {
        return new HarvestedRecordsDAO(dpsCassandraProvider());
    }


    @Bean
    public CassandraTaskErrorsDAO taskErrorDAO() {
        return CassandraTaskErrorsDAO.getInstance(dpsCassandraProvider());
    }

    @Bean
    public TasksByStateDAO tasksByStateDAO() {
        return new TasksByStateDAO(dpsCassandraProvider());
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
    public TaskStatusUpdater taskStatusUpdater() {
        return new TaskStatusUpdater(taskInfoDAO(), tasksByStateDAO(), applicationIdentifier());
    }

    @Bean
    public TaskStatusSynchronizer taskStatusSynchronizer() {
        return new TaskStatusSynchronizer(taskInfoDAO(), tasksByStateDAO());
    }

    @Bean
    public RecordStatusUpdater recordStatusUpdater(CassandraSubTaskInfoDAO cassandraSubTaskInfoDAO) {
        return new RecordStatusUpdater(cassandraSubTaskInfoDAO);
    }

    @Bean
    public MCSTaskSubmitter mcsTaskSubmitter() {
        String mcsLocation=environment.getProperty(JNDI_KEY_MCS_LOCATION);
        return new MCSTaskSubmitter(taskStatusChecker(), taskStatusUpdater(), recordSubmitService(), mcsLocation);
    }

    @Bean
    public FileURLCreator fileURLCreator(){
        String machineLocation = environment.getProperty(JNDI_KEY_MACHINE_LOCATION);
        if(machineLocation == null) {
            throw new RuntimeException(String.format("Property '%s' must be set in configuration file", JNDI_KEY_MACHINE_LOCATION));
        }
        return new FileURLCreator(machineLocation);
    }

}
