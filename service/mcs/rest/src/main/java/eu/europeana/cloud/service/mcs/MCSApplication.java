package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.service.mcs.config.AuthenticationConfiguration;
import eu.europeana.cloud.service.mcs.config.AuthorizationConfiguration;
import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import eu.europeana.cloud.service.mcs.config.PersistenceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({ServiceConfiguration.class, AuthenticationConfiguration.class, AuthorizationConfiguration.class, PersistenceConfiguration.class})
public class MCSApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(MCSApplication.class);

  public static void main(String[] args) {
    LOGGER.info("DPS Rest Application starting...");
    SpringApplication.run(MCSApplication.class, args);
  }
}
