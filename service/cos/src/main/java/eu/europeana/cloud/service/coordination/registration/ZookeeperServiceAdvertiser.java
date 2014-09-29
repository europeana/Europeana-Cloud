package eu.europeana.cloud.service.coordination.registration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;

import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.ServletContextAware;

import com.google.common.base.Throwables;

import eu.europeana.cloud.service.coordination.ServiceProperties;
import eu.europeana.cloud.service.coordination.ZookeeperService;

/**
 * Registers services as available in Zookeeper.
 * 
 * ZooKeeper is used for service discovery:
 * 
 * Services are registered on a common znode, and any client can query Zookeeper
 * for a list of available services.
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public final class ZookeeperServiceAdvertiser implements
		EcloudServiceAdvertiser {

	@Autowired
	ServletContext servletContext;

	/** Logging */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ZookeeperServiceAdvertiser.class);

	/**
	 * List of properties that allow a client to connect to this UIS REST
	 * Service.
	 */
	private ServiceProperties currentlyAdvertisedServiceProperties;

	/**
	 * Used to serialize the {@link ServiceProperties} before sending them to
	 * Zookeeper.
	 */
	private final InstanceSerializer<ServiceProperties> serializer = new JsonInstanceSerializer<ServiceProperties>(
			ServiceProperties.class);

	/** Service that actually performs the advertisement. */
	private final ServiceDiscovery<ServiceProperties> discovery;

	/**
	 * List of properties that allow a client to connect to some UIS REST
	 * Service.
	 */
	private final ServiceProperties serviceProperties;

	ZookeeperServiceAdvertiser(final ZookeeperService zookeeper,
			final String discoveryPath,
			final ServiceProperties serviceProperties) {

		LOGGER.info("ZookeeperServiceAdvertiser starting...");

		this.serviceProperties = serviceProperties;

		discovery = ServiceDiscoveryBuilder.builder(ServiceProperties.class)
				.basePath(zookeeper.getZookeeperPath() + discoveryPath)
				.client(zookeeper.getClient()).serializer(serializer).build();

		try {
			discovery.start();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}

		LOGGER.info("ZookeeperServiceAdvertiser started successfully.");
	}

	/**
	 * Registers this service as available.
	 * 
	 * Other clients querying Zookeeper for available services will receive this
	 * service on their list.
	 * 
	 * @param serviceProperties
	 *            List of properties required to connect to this Service.
	 */
	@Override
	public void startAdvertising(final ServiceProperties serviceProperties) {

		LOGGER.info("ZookeeperServiceAdvertiser starting advertising process '{}' ...",
				serviceProperties);

		try {
			discovery.registerService(convert(serviceProperties));
			this.currentlyAdvertisedServiceProperties = serviceProperties;
			LOGGER.info("ZookeeperServiceAdvertiser has advertised the service successfully.");
			
		} catch (final Exception e) {
			this.currentlyAdvertisedServiceProperties = null;
			LOGGER.error(e.getMessage());
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void stopAdvertising() {
		try {
			discovery
					.unregisterService(convert(this.currentlyAdvertisedServiceProperties));
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			throw Throwables.propagate(e);
		}
	}

	private static ServiceInstance<ServiceProperties> convert(
			final ServiceProperties p) throws Exception {
		return ServiceInstance.<ServiceProperties> builder()
				.name(p.getServiceName()).payload(p)
				.address(p.getListenAddress()).build();
	}

	/**
	 * Used if no IP address is specified by the user.
	 * 
	 * @return tries to autodetect server's ip address using ServiceInstanceBuilder.getAllLocalIPs()
	 */
	private String getAutoListenAddress() throws UnknownHostException, IOException {

		Collection<InetAddress> localIps = ServiceInstanceBuilder.getAllLocalIPs();
		Iterator<InetAddress> localIpsIter = localIps.iterator();
		LOGGER.info(
				"ZookeeperServiceAdvertiser: autodetecting ip address.. found {} localIpAddresses",
				localIps.size());

		while (localIpsIter.hasNext()) {
			InetAddress localIp = localIpsIter.next();
			if (localIp.isReachable(5000)) {
				String address = localIp.getLocalHost().getHostAddress();
				LOGGER.info("ZookeeperServiceAdvertiser: autodetected {} as listen address",
						address);
				return address;
			}
		}
		return null;
	}

	/**
	 * @return Servlet's context path. 
	 * 
	 * (Appended to the ip address, to construct the listen address for this service)
	 */
	private String getServletContextPath() {

		String servletPath = servletContext.getContextPath();
		LOGGER.info("ZookeeperServiceAdvertiser detecting ContextPath: {}", servletPath);
		return servletPath;
	}
	
	/**
	 * TODO
	 * 
	 * Used if no port is specified by the user.
	 * 
	 * @return Currently hardcoded port number.
	 * 
	 * How this port is configured will depend
	 * on how the services are going to be deployed in the future.
	 */
	private String getPort() {
		return "8080";
	}

	@PostConstruct
	public void postConstruct() {

		try {
			if (this.serviceProperties.getListenAddress().isEmpty()) {
				String autodectedIpaddress = getAutoListenAddress();
				if (autodectedIpaddress != null) {
					this.serviceProperties.setListenAddress("http://"
							+ getAutoListenAddress() + ":" + getPort()
							+ getServletContextPath());
				}
			}
			else {
				this.serviceProperties.setListenAddress(serviceProperties.getListenAddress()
						+ getServletContextPath());
			}
		}
		catch (Exception e) {
			LOGGER.warn("ZookeeperServiceAdvertiser: Error while setting service address.. {}",
					e.getMessage());
		}

		this.startAdvertising(serviceProperties);
		LOGGER.info("ZookeeperServiceAdvertiser advertising process successfull.");
	}
}
