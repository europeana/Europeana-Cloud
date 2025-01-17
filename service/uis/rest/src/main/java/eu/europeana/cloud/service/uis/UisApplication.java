package eu.europeana.cloud.service.uis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Starting point for UIS application
 */
@SpringBootApplication
public class UisApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(UisApplication.class);

    /**
     * Main entry point for the UIS application
     *
     * @param args arguments for the UIS application
     */
    public static void main(String[] args) {
        LOGGER.info("UIS Rest Application starting...");
        LOGGER.warn("Testing deployment scripts 2025 no 2 UIS");
        SpringApplication.run(UisApplication.class, args);
    }
}
