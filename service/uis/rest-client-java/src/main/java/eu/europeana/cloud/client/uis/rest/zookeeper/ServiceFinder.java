package eu.europeana.cloud.client.uis.rest.zookeeper;

import java.util.Collection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import eu.europeana.cloud.common.service.ServiceProperties;

/**
 * Asks Zookeeper for available UIS services. 
 * 
 * ZooKeeper is used for service discovery:
 * 
 * Services are registered on a common znode, and any client can query Zookeeper
 * for a list of available services.
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class ServiceFinder {

    private final ServiceDiscovery<ServiceProperties> discovery;
    
    private final static String SERVICE_NAME = "UIS";
    
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceFinder.class);

    public ServiceFinder(final CuratorFramework curatorFramework,
    		final InstanceSerializer<ServiceProperties> instanceSerializer,
    		final String zookeeperRegistrationPath) {
    	
        discovery = ServiceDiscoveryBuilder.builder(ServiceProperties.class)
            .basePath(zookeeperRegistrationPath)
            .client(curatorFramework)
            .serializer(instanceSerializer)
            .build();
 
        try {
            discovery.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
    
    public ServiceInstance<ServiceProperties> getRandomService() {
        return getService(new RandomStrategy<ServiceProperties>());
    }

    public ServiceInstance<ServiceProperties> getRoundRobinService() {
        return getService(new RoundRobinStrategy<ServiceProperties>());
    }

    public ServiceInstance<ServiceProperties> getService() {
        return getRoundRobinService();
    }

    private ServiceInstance<ServiceProperties> getService(final ProviderStrategy<ServiceProperties> loadBalancingStrategy) {

        try {
            ServiceProvider<ServiceProperties> serviceProvider = discovery.serviceProviderBuilder()
                    .serviceName(SERVICE_NAME)
                    .providerStrategy(loadBalancingStrategy)
                    .build();
			serviceProvider.start();
			return serviceProvider.getInstance();
			
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
    
    public Collection<ServiceInstance<ServiceProperties>> getServiceInstances() {
        return getServiceInstances(SERVICE_NAME);
    }
}