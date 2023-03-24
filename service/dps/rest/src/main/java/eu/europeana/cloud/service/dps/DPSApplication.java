package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.service.dps.config.AuthenticationConfiguration;
import eu.europeana.cloud.service.dps.config.AuthorizationConfiguration;
import eu.europeana.cloud.service.dps.config.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({ServiceConfiguration.class, AuthenticationConfiguration.class, AuthorizationConfiguration.class})
public class DPSApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(DPSApplication.class);

  public static void main(String[] args) {
    LOGGER.info("DPS Rest Application starting...");
    SpringApplication.run(DPSApplication.class, args);
  }
}
