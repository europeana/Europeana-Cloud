package eu.europeana.cloud.service.dps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DPSApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(DPSApplication.class);

  public static void main(String[] args) {
    LOGGER.info("DPS Rest Application starting...");
    LOGGER.warn("Testing deployment scripts 2025 DPS");
    SpringApplication.run(DPSApplication.class, args);
  }
}
