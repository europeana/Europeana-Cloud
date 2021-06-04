package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.mock;

@Configuration
public class CleanTaskDirServiceTestContext {
    @Bean
    public CassandraTaskInfoDAO taskInfoDAO() {
        return mock(CassandraTaskInfoDAO.class);
    }
}
