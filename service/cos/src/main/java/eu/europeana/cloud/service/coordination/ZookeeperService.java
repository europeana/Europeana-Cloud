package eu.europeana.cloud.service.coordination;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;


/**
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

	/** Enables the communication with the ZooKeeper ensemble. */
	private final CuratorFramework zkClient;

	/**
	 * If the connection is temporarily lost, Curator will attempt to retry the operation 
	 * until it succeeds per the currently set retry policy.
	 */
	private final static RetryPolicy ZOOKEEPER_RETRY_POLICY = new ExponentialBackoffRetry( 1000, 3 );

	/** Base path where all services are registered. */
	private final String zookeeperPath;
	
	/**
	 * 
	 */
	public ZookeeperService(final String zookeeperAddress,
			final int zookeeperConnectionTimeout,
			final int zookeeperSessionTimeout,
			final String zookeeperPath) {
		
		LOGGER.info("ZookeeperService starting...");
		
		this.zookeeperPath = zookeeperPath;

		zkClient = CuratorFrameworkFactory.builder()
				.connectionTimeoutMs(zookeeperConnectionTimeout)
				.retryPolicy(ZOOKEEPER_RETRY_POLICY)
				.sessionTimeoutMs(zookeeperSessionTimeout)
				.connectString(zookeeperAddress).build();
		zkClient.start();
		
		// ZooKeeper paths must be explicitly created
		// Let's make sure the path exists
		try {
		    new EnsurePath(zookeeperPath).ensure(zkClient.getZookeeperClient());
			LOGGER.info("ZookeeperService started successfully.");
			
		} catch (final Exception e) {
			LOGGER.error("ZooKeeper base path '{}' not found... Exception='{}'. "
					+ " The path must be manually created in zookeeper,"
					+ " and is needed for configuration settings retrieval and service registration."
					, zookeeperPath, e.getMessage());
		    throw Throwables.propagate(e);
		}
	}
	
	/**
	 * @return Base path where all services are registered.
	 */
	public String getZookeeperPath() {
		return zookeeperPath;
	}
	
	public CuratorFramework getClient() {
		return zkClient;
	}
}
