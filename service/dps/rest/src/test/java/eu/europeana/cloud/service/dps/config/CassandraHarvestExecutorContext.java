package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.mockito.Mockito.mock;

@Configuration
@Import({HarvestsExecutor.class, RecordSubmitService.class})
public class CassandraHarvestExecutorContext {

    @Bean
    public RecordSubmitService recordSubmitService(){
        return mock(RecordSubmitService.class);
    }

    @Bean
    public TaskStatusChecker taskStatusChecker() {
        return mock(TaskStatusChecker.class);
    }

    @Bean
    public CassandraTaskInfoDAO cassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
        return new CassandraTaskInfoDAO(dbService);
    }

    @Bean
    public ProcessedRecordsDAO processedRecordsDAO(CassandraConnectionProvider dbService) {
        return new ProcessedRecordsDAO(dbService);
    }

    @Bean
    public HarvestedRecordsDAO harvestedRecordsDAO(CassandraConnectionProvider dbService) {
        return new HarvestedRecordsDAO(dbService);
    }

}
