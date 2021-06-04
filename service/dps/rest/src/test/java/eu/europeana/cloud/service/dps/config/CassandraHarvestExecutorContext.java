package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.mockito.Mockito.mock;

@Configuration
@Import({HarvestsExecutor.class, RecordSubmitService.class})
public class CassandraHarvestExecutorContext {
    protected static final String KEYSPACE = "ecloud_test";
    public static final String HOST = "localhost";
    public static final String USER = "";
    public static final String PASSWORD = "";

    @Bean
    public CassandraConnectionProvider cassandraConnectionProvider(){
        return new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
    }

    @Bean
    public RecordExecutionSubmitService recordExecutionSubmitService(){
        return mock(RecordExecutionSubmitService.class);
    }

    @Bean
    public TaskStatusChecker taskStatusChecker(CassandraTaskInfoDAO cassandraTaskInfoDAO) {
        return new TaskStatusChecker(cassandraTaskInfoDAO);
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
