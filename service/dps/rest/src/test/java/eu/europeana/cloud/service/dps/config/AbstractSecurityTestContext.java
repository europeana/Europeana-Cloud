package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.ValidationStatisticsService;
import eu.europeana.cloud.service.dps.controller.TopologiesResource;
import eu.europeana.cloud.service.dps.controller.TopologyTasksResource;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.submitters.HttpTopologyTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.MCSTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.OaiTopologyTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.OtherTopologiesTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.services.validators.TaskSubmissionValidator;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.service.TaskExecutionReportServiceImpl;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.utils.UnfinishedTasksExecutor;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
@Import({TopologyTasksResource.class, TopologiesResource.class, TaskSubmissionValidator.class,
    SubmitTaskService.class, TaskDiagnosticInfoDAO.class, TaskSubmitterFactory.class, OaiTopologyTaskSubmitter.class,
    HttpTopologyTaskSubmitter.class, OtherTopologiesTaskSubmitter.class, TaskStatusUpdater.class,
    TaskStatusSynchronizer.class, MCSTaskSubmitter.class, RecordSubmitService.class, FileURLCreator.class})
public class AbstractSecurityTestContext {


  @Bean
  public HarvestsExecutor harvesterExecutor() {
    return Mockito.mock(HarvestsExecutor.class);
  }

  @Bean
  public String mcsLocation() {
    return "http://mcsLocation.com";
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
  public FilesCounter filesCounter() {
    return Mockito.mock(FilesCounter.class);
  }

  @Bean
  public TaskExecutionReportServiceImpl dpsReportService() {
    return Mockito.mock(TaskExecutionReportServiceImpl.class);
  }

  @Bean
  public ValidationStatisticsService statisticsService() {
    return Mockito.mock(ValidationStatisticsService.class);
  }

  @Bean
  public CassandraTaskInfoDAO taskDAO() {
    return Mockito.mock(CassandraTaskInfoDAO.class);
  }

  @Bean
  public CassandraTaskErrorsDAO cassandraTaskErrorsDAO() {
    return Mockito.mock(CassandraTaskErrorsDAO.class);
  }

  @Bean
  public FilesCounterFactory filesCounterFactory() {
    return Mockito.mock(FilesCounterFactory.class);
  }

  @Bean
  public UnfinishedTasksExecutor UnfinishedTasksExecutor() {
    return Mockito.mock(UnfinishedTasksExecutor.class);
  }

  @Bean
  public TasksByStateDAO tasksDAO() {
    return Mockito.mock(TasksByStateDAO.class);
  }

  @Bean
  public TaskSubmitter depublicationTaskSubmitter() {
    return Mockito.mock(TaskSubmitter.class);
  }

  @Bean
  @Scope("prototype")
  public ThreadPoolTaskExecutor taskExecutor() {
    return new ThreadPoolTaskExecutor();
  }

}
