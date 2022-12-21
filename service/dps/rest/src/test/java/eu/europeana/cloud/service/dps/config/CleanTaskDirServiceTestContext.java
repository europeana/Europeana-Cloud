package eu.europeana.cloud.service.dps.config;

import static org.mockito.Mockito.mock;

import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CleanTaskDirServiceTestContext {

  @Bean
  public CassandraTaskInfoDAO taskInfoDAO() {
    return mock(CassandraTaskInfoDAO.class);
  }
}
