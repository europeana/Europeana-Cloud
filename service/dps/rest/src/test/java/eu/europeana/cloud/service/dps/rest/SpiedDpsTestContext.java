package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.dps.rest.exceptionmappers.UnitedExceptionMapper;
import eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.kafka.TaskKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraKillService;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraReportService;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraValidationStatisticsService;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
@Import({UnitedExceptionMapper.class, SubmitTaskThread.class})
public class SpiedDpsTestContext {

    /* REAL Beans */
    @Bean
    @Scope("prototype")
    public ThreadPoolTaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor();
    }

    @Bean
    public String mcsLocation() {
        return "test";
    }


    /* MOCKED Beans */
    @Bean
    public ApplicationContext applicationContext() {
        return Mockito.mock(ApplicationContext.class);
    }

    @Bean
    public CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO() {
        return Mockito.mock(CassandraNodeStatisticsDAO.class);
    }

    @Bean
    public PermissionManager permissionManger() {
        return Mockito.mock(PermissionManager.class);
    }

    @Bean
    public CassandraReportService reportService() {
        return Mockito.mock(CassandraReportService.class);
    }

    @Bean
    public CassandraValidationStatisticsService validationStatisticsService() {
        return Mockito.mock(CassandraValidationStatisticsService.class);
    }

    @Bean
    public TaskKafkaSubmitService submitService() {
        return Mockito.mock(TaskKafkaSubmitService.class);
    }

    @Bean
    public RecordKafkaSubmitService recordKafkaSubmitService() {
        return Mockito.mock(RecordKafkaSubmitService.class);
    }

    @Bean
    public CassandraKillService killService() {
        return Mockito.mock(CassandraKillService.class);
    }

    @Bean
    public TopologyManager topologyManager() {
        return Mockito.mock(TopologyManager.class);
    }

    @Bean
    public MutableAclService mutableAclService() {
        return Mockito.mock(MutableAclService.class);
    }

    @Bean
    public RecordServiceClient recordServiceClient() {
        return Mockito.mock(RecordServiceClient.class);
    }

    @Bean
    public FileServiceClient fileServiceClient() {
        return Mockito.mock(FileServiceClient.class);
    }

    @Bean
    public DataSetServiceClient dataSetServiceClient() {
        return Mockito.mock(DataSetServiceClient.class);
    }

    @Bean
    public CassandraTaskInfoDAO taskDAO() {
        return Mockito.mock(CassandraTaskInfoDAO.class);
    }

    @Bean
    public FilesCounterFactory filesCounterFactory() {
        return Mockito.mock(FilesCounterFactory.class);
    }

    @Bean
    public FilesCounter filesCounter() {
        return Mockito.mock(FilesCounter.class);
    }

    @Bean
    public UrlParser urlParser() {
        return Mockito.mock(UrlParser.class);
    }

    @Bean
    public TasksByStateDAO tasksDAO() {
        return Mockito.mock(TasksByStateDAO.class);
    }

    @Bean
    public ProcessedRecordsDAO processedRecordsDAO() {
        return Mockito.mock(ProcessedRecordsDAO.class);
    }

    @Bean
    public TaskStatusChecker taskStatusChecker() {
        return Mockito.mock(TaskStatusChecker.class);
    }

    @Bean
    public KafkaTopicSelector kafkaTopicSelector() {
        return Mockito.mock(KafkaTopicSelector.class);
    }

    @Bean
    public HarvestsExecutor harvesterExecutor() {
        return Mockito.mock(HarvestsExecutor.class);
    }
}
