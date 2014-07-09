package eu.europeana.cloud.service.coordination.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import eu.europeana.cloud.service.coordination.ServiceProperties;
import eu.europeana.cloud.service.coordination.ZookeeperService;

/**
 * Asks Zookeeper for available services. 
 * 
 * ZooKeeper is used for service discovery:
 * 
 * Services are registered on a common znode, and any client can query Zookeeper
 * for a list of available services.
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class ZookeeperServiceDiscovery implements EcloudServiceDiscovery {

    /** Type of service to search for, e.g UIS. */
    private final String serviceType;
    
    /** Service that actually performs the discovery. */
    private final ServiceDiscovery<ServiceProperties> discovery;
    
    /** Logging */
	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperServiceDiscovery.class);
	
    private final ServiceCache<ServiceProperties> cache;
	
	/**
	 * @param zookeeper service that provides the connection with Zookeeper.
	 * @param serviceType Type of service to search for, e.g UIS.
	 */
    public ZookeeperServiceDiscovery(final ZookeeperService zookeeper, final String serviceType) {
    	
    	this.serviceType = serviceType;

		final InstanceSerializer<ServiceProperties> instanceSerializer = new JsonInstanceSerializer<ServiceProperties>(ServiceProperties.class);
    	
        discovery = ServiceDiscoveryBuilder.builder(ServiceProperties.class)
            .basePath(zookeeper.getZookeeperPath())
            .client(zookeeper.getClient())
            .serializer(instanceSerializer)
            .build();
        
        cache = discovery.serviceCacheBuilder().name(serviceType).build();
 
        try {
        	cache.start();
            discovery.start();
        } catch (final Exception e) {
        	LOGGER.error("ZookeeperServiceDiscovery error starting the service.. {}", e.getMessage());
            throw Throwables.propagate(e);
        }
    }
    
	@Override
    public List<ServiceProperties> getServices() {

        try {
            ServiceProvider<ServiceProperties> serviceProvider = discovery.serviceProviderBuilder()
                    .serviceName(serviceType)
                    .build();
			serviceProvider.start();
			
			Collection<ServiceInstance<ServiceProperties>> serviceInstances = serviceProvider.getAllInstances();
			List<ServiceProperties> services = new ArrayList<ServiceProperties>();
			
			for(Iterator<ServiceInstance<ServiceProperties>> i = serviceInstances.iterator(); i.hasNext(); ) {
				services.add(i.next().getPayload());
			}
			return services;
			
		} catch (Exception e) {
        	LOGGER.error(e.getMessage());
            throw Throwables.propagate(e);
		}
    }
 
    private Collection<ServiceInstance<ServiceProperties>> getServiceInstances(final String serviceName) {
    	
        Collection<ServiceInstance<ServiceProperties>> instances;
 
        try {
            instances = discovery.queryForInstances(serviceName);
        	final int servicesDiscoveredCount = instances.size();

        	LOGGER.info("Found '{}' services: {} .", servicesDiscoveredCount, instances);
            
        } catch (Exception e) {
        	LOGGER.error(e.getMessage());
            throw Throwables.propagate(e);
        }
 
        return instances;
    }
}