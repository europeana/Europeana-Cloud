package eu.europeana.cloud.service.commons.logging;

import kafka.producer.KafkaLog4jAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jndi.JndiTemplate;

import javax.annotation.PostConstruct;
import javax.naming.NamingException;
import java.util.Enumeration;

/**
 * 
 * Inserts unique instance name into log message based on machine hostname;<br/>
 *
 */
public class SimpleLoggingConfigurator extends LoggingConfigurator {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SimpleLoggingConfigurator.class);
    
    private static final String EUROPEANA_CLOUD_LOGGER_NAME ="eu.europeana.cloud.service.commons.logging.LoggingFilter";
    private static final String KAFKA_APPENDER_NAME = "KAFKA";
    private static final String KAFKA_BROKER_LIST_JNDI_NAME = "java:comp/env/logging/kafka/brokerList";
    
    @Override
    @PostConstruct
    public void configure() {
        LOGGER.info("Start configuring logging system");
        String hostname = readHostname(null);
        
        configureKafkaLogger(hostname);
        configureRootLogger(hostname);

        LOGGER.info("Logging system configuration finished");
    }

    private void configureKafkaLogger(String hostname) {
        Logger europeanaLogger = Logger.getLogger(EUROPEANA_CLOUD_LOGGER_NAME);
        try{
            if(isKafkaAppenderDefined(europeanaLogger)){
                configureKafkaAppender(europeanaLogger);
            }
        }catch(NamingException ex){
            removeKafkaAppenderFromLogger(europeanaLogger);
            LOGGER.error("Name " +KAFKA_BROKER_LIST_JNDI_NAME +" not found in JNDI.");
        }
        turnOnLogger(europeanaLogger);
        loggerUpdater.update(europeanaLogger, APPLICATION_INSTANCE_NAME_MARKER, hostname);
    }

    private void configureRootLogger(String hostname) {
        Logger applicationRootLogger = Logger.getRootLogger();
        loggerUpdater.update(applicationRootLogger, APPLICATION_INSTANCE_NAME_MARKER, hostname);
    }

    private boolean isKafkaAppenderDefined(Logger logger) {
        if (logger != null) {
            Appender kafkaAppender = logger.getAppender(KAFKA_APPENDER_NAME);
            if (kafkaAppender != null) {
                return true;
            } else {
                return false;
            }
        }else{
            return false;
        }
    }
    
    private void configureKafkaAppender(Logger logger) throws NamingException {
        Enumeration<Appender> appenderEnumerator = logger.getAllAppenders();
        while (appenderEnumerator.hasMoreElements()) {
            Appender appender = appenderEnumerator.nextElement();
            if(appender instanceof KafkaLog4jAppender){
                KafkaLog4jAppender kafkaAppender = (KafkaLog4jAppender)appender;
                String brokerList = readKafkaBrokerListFromJNDI();
                kafkaAppender.setBrokerList(brokerList);
                kafkaAppender.activateOptions();
            }
        }
    }
    
    private String readKafkaBrokerListFromJNDI() throws NamingException {
        JndiTemplate jndi = new JndiTemplate();
        return jndi.lookup(KAFKA_BROKER_LIST_JNDI_NAME).toString();
    }

    private void removeKafkaAppenderFromLogger(Logger logger) {
        logger.removeAppender(KAFKA_APPENDER_NAME);
    }

    private void turnOnLogger(Logger logger) {
        logger.setLevel(Level.INFO);
    }
}
