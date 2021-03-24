package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordDAO;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.mockito.Mockito.mock;

@Configuration
@Import({HarvestsExecutor.class, RecordSubmitService.class, HarvestedRecordDAO.class, ProcessedRecordsDAO.class,
         CassandraTaskInfoDAO.class})
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

}
