package eu.europeana.cloud.service.mcs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MCSApplication {
  private static final Logger LOGGER = LoggerFactory.getLogger(MCSApplication.class);

  public static void main(String[] args) {
    LOGGER.info("MCS Rest Application starting...");
    SpringApplication.run(MCSApplication.class, args);
  }
}
