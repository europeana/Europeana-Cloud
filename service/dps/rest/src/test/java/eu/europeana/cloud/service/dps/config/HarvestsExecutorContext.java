package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HarvestsExecutorContext {

    @Bean
    public RecordExecutionSubmitService recordExecutionSubmitService() {
        return Mockito.mock(RecordExecutionSubmitService.class);
    }

    @Bean
    public ProcessedRecordsDAO processedRecordsDAO() {
        return Mockito.mock(ProcessedRecordsDAO.class);
    }

    @Bean
    public HarvestedRecordsDAO harvestedRecordsDAO() {
        return Mockito.mock(HarvestedRecordsDAO.class);
    }


    @Bean
    public TaskStatusChecker taskStatusChecker() {
        return Mockito.mock(TaskStatusChecker.class);
    }

    @Bean
    public RecordSubmitService recordSubmitService() {
        return new RecordSubmitService(processedRecordsDAO(), recordExecutionSubmitService());
    }


}
