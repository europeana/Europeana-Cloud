package eu.europeana.cloud.service.uis.registration;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import eu.europeana.cloud.common.service.ServiceProperties;


/**
 * Registers services as available in Zookeeper. 
 * 
 * 
 * ZooKeeper is used for service discovery:
 * 
 * Services are registered on a common znode, and any client can query Zookeeper
 * for a list of available services.
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public final class ZookeeperServiceAdvertiser {
	
	/** Logging */
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperService.class);
	
	/** List of properties that allow a client to connect to this UIS REST Service. */
	private ServiceProperties currentlyAdvertisedServiceProperties;

	/** Used to serialize the {@link ServiceProperties} before sending them to Zookeeper. */
	private final InstanceSerializer<ServiceProperties> jsonInstanceSerializer;

	/** Enables the communication with Zookeeper. */
	private final CuratorFramework curatorFramework;
	
	/** zNode for service registration */
	private final String zNode;
	
	ZookeeperServiceAdvertiser(final CuratorFramework curatorFramework, 
			final InstanceSerializer<ServiceProperties> instanceSerializer,
			final String zNode) {
		
		LOGGER.info("ZookeeperServiceAdvertiser starting...");
		
		this.curatorFramework = curatorFramework;
		this.jsonInstanceSerializer = instanceSerializer;
		this.zNode = zNode;
		this.currentlyAdvertisedServiceProperties = null;

		// ZooKeeper paths must be explicitly created
		// Let's make sure the path exists
		try {
		    new EnsurePath(zNode).ensure(curatorFramework.getZookeeperClient());
		} catch (final Exception e) {
			LOGGER.error("ZooKeeper path='{}' not found... Exception='{}'", zNode, e.getMessage());
		    throw Throwables.propagate(e);
		}
		
		LOGGER.info("ZookeeperServiceAdvertiser started successfully.");
	}
	
	/**
	 * Registers this service as available.
	 * Other clients querying Zookeeper for available services will receive this service on their list.
	 * 
	 * @param serviceProperties List of properties required to connect to this Service.
	 */
	public void startAdvertising(final ServiceProperties serviceProperties) {
		
		LOGGER.info("ZookeeperServiceAdvertiser startAdvertising...");
		try {
			ServiceDiscovery<ServiceProperties> discovery = getDiscovery();
			discovery.start();
			discovery.registerService(convert(serviceProperties));
			this.currentlyAdvertisedServiceProperties = serviceProperties;
		} catch (final Exception e) {
			this.currentlyAdvertisedServiceProperties = null;
			LOGGER.error(e.getMessage());
			throw Throwables.propagate(e);
		}
		LOGGER.info("ZookeeperServiceAdvertiser has advertised the service successfully.");
	}

	public void stopAdvertising() {
		try {
			ServiceDiscovery<ServiceProperties> discovery = getDiscovery();
			discovery.start();
			discovery.unregisterService(convert(this.currentlyAdvertisedServiceProperties));
			discovery.close();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			throw Throwables.propagate(e);
		}
	}

	private static ServiceInstance<ServiceProperties> convert(final ServiceProperties p) throws Exception {
		return ServiceInstance.<ServiceProperties>builder()
				.name(p.getServiceName())
				.payload(p)
				.address(p.getListenAddress())
				.id(p.getServiceId())
				.build();
	}

	private ServiceDiscovery<ServiceProperties> getDiscovery() {
		return ServiceDiscoveryBuilder.builder(ServiceProperties.class)
				.basePath(zNode)
				.client(curatorFramework)
				.serializer(jsonInstanceSerializer)
				.build();
	}
}
