package eu.europeana.cloud.service.mcs.persistent.context;

import static org.mockito.Mockito.mock;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class UisIntegrationTestContext {

  @Bean
  public UISClientHandlerImpl uisClientHandler() {
    return new UISClientHandlerImpl();
  }

  @Bean
  public UISClient uisClient() {
    return mock(UISClient.class);
  }
}
