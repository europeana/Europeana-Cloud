package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RecordContext {

  @Bean
  public RecordExecutionSubmitService recordExecutionSubmitService() {
    return Mockito.mock(RecordExecutionSubmitService.class);
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
  public TaskDiagnosticInfoDAO taskDiagnosticInfoDAO() {
    return Mockito.mock(TaskDiagnosticInfoDAO.class);
  }

}
