package eu.europeana.cloud.service.commons.logging;

import eu.europeana.cloud.service.coordination.registration.ZookeeperServiceAdvertiser;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.net.Inet4Address;
import java.net.UnknownHostException;

public class LoggingConfigurator {

    private static final String APPLICATION_INSTANCE_NAME_MARKER = "instanceName";

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LoggingConfigurator.class);

    private LoggerUpdater loggerUpdater;
    private ZookeeperServiceAdvertiser serviceAdvertiser;

    public LoggingConfigurator(ZookeeperServiceAdvertiser serviceAdvertiser){
        this.serviceAdvertiser = serviceAdvertiser;
        loggerUpdater = new LoggerUpdater();
    }

    @PostConstruct
    public void configure(){

        LOGGER.info("Start configuring logging system");
        String hostname = readHostname(serviceAdvertiser);

        Logger applicationRootLogger = Logger.getRootLogger();
        loggerUpdater.update(applicationRootLogger, APPLICATION_INSTANCE_NAME_MARKER, hostname);

        LOGGER.info("Logging system configuration finished");
    }

    private String readHostname(ZookeeperServiceAdvertiser serviceAdvertiser){
        try{
            if(serviceAdvertiser == null){
                return Inet4Address.getLocalHost().getHostName();
            }else{
                return serviceAdvertiser.getServiceProperties().getServiceUniqueName();
            }
        }catch(UnknownHostException e){
            return "";
        }
    }
}
