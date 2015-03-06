package eu.europeana.cloud.service.commons.logging;

import eu.europeana.cloud.service.coordination.registration.ZookeeperServiceAdvertiser;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.net.Inet4Address;
import java.net.UnknownHostException;

public abstract class LoggingConfigurator {

    protected static final String APPLICATION_INSTANCE_NAME_MARKER = "instanceName";

    public abstract void configure();

    protected LoggerUpdater loggerUpdater = new LoggerUpdater();

    protected String readHostname(ZookeeperServiceAdvertiser serviceAdvertiser) {

        try {
            if (serviceAdvertiser == null) {
                return Inet4Address.getLocalHost().getHostName();
            } else {
                return serviceAdvertiser.getServiceProperties().getServiceUniqueName();
            }
        } catch (UnknownHostException e) {
            return "";
        }
    }

    protected String readHostname() {
        try {
            return Inet4Address.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "";
        }
    }
}
