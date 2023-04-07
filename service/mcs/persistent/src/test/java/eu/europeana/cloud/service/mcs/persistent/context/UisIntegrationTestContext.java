package eu.europeana.cloud.service.mcs.persistent.context;

import static org.mockito.Mockito.mock;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UisIntegrationTestContext {

  @Bean
  public UISClientHandlerImpl uisClientHandler() {
    return new UISClientHandlerImpl(uisClient());
  }

  @Bean
  public UISClient uisClient() {
    return mock(UISClient.class);
  }
}
