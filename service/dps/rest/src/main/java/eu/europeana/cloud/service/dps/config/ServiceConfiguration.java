package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.kafka.TaskKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.services.postprocessors.HarvestingPostProcessor;
import eu.europeana.cloud.service.dps.services.postprocessors.IndexingPostProcessor;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingService;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessorFactory;
import eu.europeana.cloud.service.dps.services.submitters.MCSTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.service.ReportService;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.config.JndiNames.*;

@Configuration
@EnableWebMvc
@PropertySource("classpath:dps.properties")
@ComponentScan("eu.europeana.cloud.service.dps")
@EnableAspectJAutoProxy
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
    public ReportService taskReportService() {
        return new ReportService(
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
        var result = new MethodInvokingFactoryBean();
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
    public TaskDiagnosticInfoDAO taskDiagnosticInfoDAO() {
        return new TaskDiagnosticInfoDAO(dpsCassandraProvider());
    }

    @Bean
    public HarvestedRecordsDAO harvestedRecordsDAO() {
        return new HarvestedRecordsDAO(dpsCassandraProvider());
    }


    @Bean
    public CassandraTaskErrorsDAO taskErrorDAO() {
        return new CassandraTaskErrorsDAO(dpsCassandraProvider());
    }

    @Bean
    public TasksByStateDAO tasksByStateDAO() {
        return new TasksByStateDAO(dpsCassandraProvider());
    }

    @Bean
    public ValidationStatisticsServiceImpl validationStatisticsService() {
        return new ValidationStatisticsServiceImpl(
                cassandraGeneralStatisticsDAO(),
                cassandraNodeStatisticsDAO(),
                cassandraAttributeStatisticsDAO(),
                cassandraStatisticsReportDAO());
    }

    @Bean
    public GeneralStatisticsDAO cassandraGeneralStatisticsDAO() {
        return new GeneralStatisticsDAO(dpsCassandraProvider());
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
    public StatisticsReportDAO cassandraStatisticsReportDAO() {
        return new StatisticsReportDAO(dpsCassandraProvider());
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
        return new TaskStatusSynchronizer(taskInfoDAO(), tasksByStateDAO(), taskStatusUpdater());
    }

    @Bean
    public RecordStatusUpdater recordStatusUpdater(CassandraSubTaskInfoDAO cassandraSubTaskInfoDAO) {
        return new RecordStatusUpdater(cassandraSubTaskInfoDAO);
    }

    @Bean
    public MCSTaskSubmitter mcsTaskSubmitter() {
        return new MCSTaskSubmitter(taskStatusChecker(), taskStatusUpdater(), recordSubmitService(), mcsLocation());
    }

    @Bean
    public FileURLCreator fileURLCreator(){
        String machineLocation = environment.getProperty(JNDI_KEY_MACHINE_LOCATION);
        if(machineLocation == null) {
            throw new BeanCreationException(String.format("Property '%s' must be set in configuration file", JNDI_KEY_MACHINE_LOCATION));
        }
        return new FileURLCreator(machineLocation);
    }

    @Bean
    public PostProcessorFactory postProcessorFactory() {
        return new PostProcessorFactory(
                Arrays.asList(harvestingPostProcessor(), indexingPostProcessor())
        );
    }

    @Bean
    public HarvestingPostProcessor harvestingPostProcessor(){
        return new HarvestingPostProcessor(harvestedRecordsDAO(), processedRecordsDAO(),
                recordServiceClient(), revisionServiceClient(), uisClient(), dataSetServiceClient(), taskStatusUpdater());
    }

    @Bean
    public IndexingPostProcessor indexingPostProcessor(){
        return new IndexingPostProcessor(taskStatusUpdater(), harvestedRecordsDAO());
    }

    @Bean
    public UISClient uisClient() {
        return new UISClient(uisLocation());
    }

    @Bean
    public DataSetServiceClient dataSetServiceClient() {
        return new DataSetServiceClient(mcsLocation());
    }

    @Bean
    public RecordServiceClient recordServiceClient() {
        return new RecordServiceClient(mcsLocation());
    }

    @Bean
    public RevisionServiceClient revisionServiceClient() {
        return new RevisionServiceClient(mcsLocation());
    }

    private String mcsLocation() {
        return environment.getProperty(JNDI_KEY_MCS_LOCATION);
    }

    private String uisLocation() {
        return environment.getProperty(JNDI_KEY_UIS_LOCATION);
    }

    @Bean
    public RetryAspect retryAspect() {
        return new RetryAspect();
    }

    @Bean
    public PostProcessingService postProcessingService() {
        return new PostProcessingService(postProcessorFactory(), taskInfoDAO(), taskDiagnosticInfoDAO(),
                tasksByStateDAO(), taskStatusUpdater(), applicationIdentifier());
    }

}
