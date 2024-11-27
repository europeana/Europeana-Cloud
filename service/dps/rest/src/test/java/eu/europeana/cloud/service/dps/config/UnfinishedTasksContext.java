package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UnfinishedTasksContext {

  @Bean
  public TasksByStateDAO tasksDAO() {
    return Mockito.mock(TasksByStateDAO.class);
  }

  @Bean
  public TaskDiagnosticInfoDAO taskDiagnosticInfoDAO() {
    return Mockito.mock(TaskDiagnosticInfoDAO.class);
  }

  @Bean
  public String applicationIdentifier() {
    return "exampleAppIdentifier";
  }

  @Bean
  public CassandraTaskInfoDAO cassandraTaskInfoDAO() {
    return Mockito.mock(CassandraTaskInfoDAO.class);
  }

  @Bean
  public TaskStatusUpdater taskStatusUpdater() {
    return Mockito.mock(TaskStatusUpdater.class);
  }

  @Bean
  public TaskSubmitterFactory taskSubmitter() {
    return Mockito.mock(TaskSubmitterFactory.class);
  }

  @Bean
  public SubmitTaskService submitTaskService() {
    return Mockito.mock(SubmitTaskService.class);
  }
}
