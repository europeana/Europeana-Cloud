package eu.europeana.cloud.client.uis.rest.zookeeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import eu.europeana.cloud.common.service.ServiceProperties;

/**
 * 
 * Enables and constantly monitors the communication with Zookeeper by managing
 * a connection to the ZooKeeper ensemble. 
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public class ZookeeperService {

	/** Logging */
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperService.class);

	/** Enables and constantly monitors the communication with the ZooKeeper ensemble. */
	private final CuratorFramework curatorFramework;
	
	private final static int ZOOKEEPER_CONNECTION_TIMEOUT = 60000;
	
	private final static int ZOOKEEPER_SESSION_TIMEOUT = 60000;
	
	private final ServiceFinder serviceFinder;

	/**
	 * If the connection is temporarily lost, Curator will attempt to retry the operation 
	 * until it succeeds per the currently set retry policy.
	 */
	private final static RetryPolicy ZOOKEEPER_RETRY_POLICY = new ExponentialBackoffRetry( 1000, 3 );
	
	public ZookeeperService() throws IOException {
		
		LOGGER.info("ZookeeperService starting...");

		final String zookeeperAddress = getZookeeperAddress();
		LOGGER.info("ZookeeperService using Zookeeper Address='{}'", zookeeperAddress);
		final String zNode = getZookeeperZNode();
		LOGGER.info("ZookeeperService using zookeeper zNode='{}'", zNode);

		curatorFramework = CuratorFrameworkFactory.builder()
				.connectionTimeoutMs(ZOOKEEPER_CONNECTION_TIMEOUT)
				.retryPolicy(ZOOKEEPER_RETRY_POLICY)
				.sessionTimeoutMs(ZOOKEEPER_SESSION_TIMEOUT)
				.connectString(zookeeperAddress).build();
		curatorFramework.start();
		
		// ZooKeeper paths must be explicitly created
		// Let's make sure the path exists
		try {
		    new EnsurePath(zNode).ensure(curatorFramework.getZookeeperClient());
		} catch (final Exception e) {
			LOGGER.error("ZooKeeper zNode='{}' not found... Exception='{}'", zNode, e.getMessage());
		    throw Throwables.propagate(e);
		}
		
		serviceFinder = new ServiceFinder(curatorFramework
				, new JsonInstanceSerializer<ServiceProperties>(ServiceProperties.class)
				, zNode);
		
		LOGGER.info("ZookeeperService started successfully.");
	}
	
	public ServiceFinder getServiceFinder() {
		return serviceFinder;
	}
	
	/**
	 * @return reads the Zookeeper Address from the property file.
	 */
	private String getZookeeperAddress() throws IOException {

		Properties props = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(new File("src/main/resources/client.properties"));
			props.load(is);
			return props.getProperty("zookeeper.URL");
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (is != null) {
				is.close();
			}
		}
		return null;
	}

	/**
	 * @return reads the default Zookeeper zNode from the property file.
	 */
	private String getZookeeperZNode() throws IOException {

		Properties props = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(new File("src/main/resources/client.properties"));
			props.load(is);
			return props.getProperty("zookeeper.ZNODE");
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (is != null) {
				is.close();
			}
		}
		return null;
	}
}
