package eu.europeana.cloud.service.commons.logging;

import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

/**
 * 
 * Inserts unique instance name into log message based on machine hostname;<br/>
 *
 */
public class SimpleLoggingConfigurator extends LoggingConfigurator {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SimpleLoggingConfigurator.class);
    
    @Override
    @PostConstruct
    public void configure() {
        LOGGER.info("Start configuring logging system");
        String hostname = readHostname(null);

        Logger applicationRootLogger = Logger.getRootLogger();
        loggerUpdater.update(applicationRootLogger, APPLICATION_INSTANCE_NAME_MARKER, hostname);

        LOGGER.info("Logging system configuration finished");
    }
}
