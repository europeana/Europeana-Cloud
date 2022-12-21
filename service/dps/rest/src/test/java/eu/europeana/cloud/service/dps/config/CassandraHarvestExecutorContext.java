package eu.europeana.cloud.service.dps.config;

import static org.mockito.Mockito.mock;

import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({HarvestsExecutor.class})
public class CassandraHarvestExecutorContext {

  @Bean
  public RecordSubmitService recordSubmitService() {
    return mock(RecordSubmitService.class);
  }

  @Bean
  public TaskStatusChecker taskStatusChecker() {
    return mock(TaskStatusChecker.class);
  }

}
