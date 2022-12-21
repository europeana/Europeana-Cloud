package eu.europeana.cloud.service.dps.depublish;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.indexing.Indexer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

@Configuration
@EnableAsync
public class TestContext {

  @Bean
  public TaskStatusChecker taskStatusChecker() {
    return mock(TaskStatusChecker.class);
  }

  @Bean
  public TaskStatusUpdater taskStatusUpdater() {
    return mock(TaskStatusUpdater.class);
  }

  @Bean
  public IndexWrapper indexWrapper() {
    IndexWrapper wrapper = mock(IndexWrapper.class);
    when(wrapper.getIndexer(TargetIndexingDatabase.PUBLISH)).thenReturn(mock(Indexer.class));
    return wrapper;
  }

  @Bean
  public RecordStatusUpdater recordStatusUpdater() {
    return mock(RecordStatusUpdater.class);
  }

  @Bean
  public HarvestedRecordsDAO harvestedRecordsDAO() {
    return mock(HarvestedRecordsDAO.class);
  }
}
