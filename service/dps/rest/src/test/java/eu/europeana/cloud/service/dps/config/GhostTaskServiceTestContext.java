package eu.europeana.cloud.service.dps.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GhostTaskServiceTestContext {

  @Bean
  public KafkaProperties kafkaProperties() {
    KafkaProperties kafkaProperties = mock(KafkaProperties.class);
    when(kafkaProperties.getTopologyAvailableTopics()).thenReturn(
        "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;validation_topology:validation_topology_1");
    return kafkaProperties;
  }

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
