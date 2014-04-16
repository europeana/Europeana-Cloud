package eu.europeana.cloud.service.uis.registration;

import javax.annotation.PostConstruct;

//import org.apache.catalina.core.StandardServer;
//import org.apache.catalina.Server;
//import org.apache.catalina.ServerFactory;
//import org.apache.catalina.Service;
//import org.apache.catalina.connector.Connector;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.service.ServiceProperties;


/**
 * 
 * Enables and constantly monitors the communication with Zookeeper by managing
 * a connection to the ZooKeeper ensemble. 
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
@Component
public class ZookeeperService {
	
	/** Logging */
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperService.class);

	/** Used to advertise this REST Service to Zookeeper */
	private final ZookeeperServiceAdvertiser advertiser;

	/** Enables the communication with the ZooKeeper ensemble. */
	private final CuratorFramework curatorFramework;

	/**
	 * If the connection is temporarily lost, Curator will attempt to retry the operation 
	 * until it succeeds per the currently set retry policy.
	 */
	private final static RetryPolicy ZOOKEEPER_RETRY_POLICY = new ExponentialBackoffRetry( 1000, 3 );

	/** Used to serialize the {@link ServiceProperties} before sending them to Zookeeper. */
	private final InstanceSerializer<ServiceProperties> serializer = new JsonInstanceSerializer<ServiceProperties>(ServiceProperties.class);

	/** List of properties that allow a client to connect to some UIS REST Service. */
	private final ServiceProperties properties;
	
	public ZookeeperService(final String zookeeperAddress,
			final int zookeeperConnectionTimeout,
			final int zookeeperSessionTimeout,
			final String zNode,
			final ServiceProperties serviceProperties) {
		
		LOGGER.info("ZookeeperService starting...");
		
		this.properties = serviceProperties;

		curatorFramework = CuratorFrameworkFactory.builder()
				.connectionTimeoutMs(zookeeperConnectionTimeout)
				.retryPolicy(ZOOKEEPER_RETRY_POLICY)
				.sessionTimeoutMs(zookeeperSessionTimeout)
				.connectString(zookeeperAddress).build();
		curatorFramework.start();
		
		advertiser = new ZookeeperServiceAdvertiser(curatorFramework, serializer, zNode);
		
		LOGGER.info("ZookeeperService started successfully.");
	}
	
    @PostConstruct
    public void postConstruct() {
    	
    	/** 
    	 * TODO: address is currently passed through Spring properties 
    	 * 
    	 * https://jira.man.poznan.pl/jira/browse/ECL-194
    	 **/
		LOGGER.info("Server address !TODO! (currently hardcoded in spring properties) == '{}'", properties.getListenAddress());
		LOGGER.info("ZookeeperService starting advertising process '{}' ...", properties);
		
		advertiser.startAdvertising(properties);
		
		LOGGER.info("ZookeeperService advertising process successfull.");
    }
}
