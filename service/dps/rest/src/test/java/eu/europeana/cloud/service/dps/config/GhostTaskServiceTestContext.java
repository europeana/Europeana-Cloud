package eu.europeana.cloud.service.dps.config;

import static org.mockito.Mockito.mock;

import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GhostTaskServiceTestContext {

  @Bean
  public CassandraTaskInfoDAO taskInfoDAO() {
    return mock(CassandraTaskInfoDAO.class);
  }

  @Bean
  public TasksByStateDAO tasksByStateDAO() {
    return mock(TasksByStateDAO.class);
  }

  @Bean
  public TaskDiagnosticInfoDAO taskDiagnosticInfoDAO() {
    return mock(TaskDiagnosticInfoDAO.class);
  }

}
