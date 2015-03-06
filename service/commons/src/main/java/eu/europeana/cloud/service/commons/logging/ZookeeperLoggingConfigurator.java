package eu.europeana.cloud.service.commons.logging;

import eu.europeana.cloud.service.coordination.registration.ZookeeperServiceAdvertiser;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.net.Inet4Address;
import java.net.UnknownHostException;

/**
 * Configures log4j logger using values stored in zookeeper.<br/><br/>
 * 1. Inserts unique instance name into log message;<br/>
 * 2. Configures kafka appender;<br/>
 */
public class ZookeeperLoggingConfigurator extends LoggingConfigurator {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ZookeeperLoggingConfigurator.class);

    private ZookeeperServiceAdvertiser serviceAdvertiser;
    private String kafkaBrokerList;
    private String kafkaTopicName;

    public ZookeeperLoggingConfigurator(
            ZookeeperServiceAdvertiser serviceAdvertiser,
            String kafkaBrokerList,
            String kafkaTopicName) {
        this.serviceAdvertiser = serviceAdvertiser;
        this.kafkaBrokerList = kafkaBrokerList;
        this.kafkaTopicName = kafkaTopicName;
    }

    @PostConstruct
    @Override
    public void configure() {

        LOGGER.info("Start configuring logging system using Zookeeper");
        String serviceName = serviceAdvertiser.getServiceProperties().getServiceName().toLowerCase();
        String hostname = readHostname(serviceAdvertiser);

        Logger applicationRootLogger = Logger.getRootLogger();
        loggerUpdater.update(applicationRootLogger, APPLICATION_INSTANCE_NAME_MARKER, hostname);
        loggerUpdater.addKafkaAppender(applicationRootLogger, kafkaBrokerList, kafkaTopicName, serviceName, hostname);

        LOGGER.info("Logging system configuration finished");
    }
}
