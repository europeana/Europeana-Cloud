package eu.europeana.cloud.service.coordination.provider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.coordination.ServiceProperties;
import eu.europeana.cloud.service.coordination.discovery.EcloudServiceDiscovery;

/**
 *  Returns random service instances from the list of all available services,
 *  but will always prefer services from {@link #preferredDatacenter} if possible.
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 * 
 */
public class ServiceProviderImpl implements ServiceProvider {
	
	/** Used to discover a list of all available services */
	private EcloudServiceDiscovery serviceDiscovery;
	
	/** This service provider will always prefer service instances located in the preferredDatacenter */
	private final String preferredDatacenter;
	
	/** In case many services are found, one will randomly be selected */
	private final Random random = new Random();
	
	/** Logging */
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProviderImpl.class);
	
	public ServiceProviderImpl(final EcloudServiceDiscovery serviceDiscovery, final String preferredDatacenter) {
		
		LOGGER.info("Starting ServiceProviderImpl...");
		
		this.serviceDiscovery = serviceDiscovery;
		this.preferredDatacenter = preferredDatacenter;

		LOGGER.info("ServiceProviderImpl started succesfully.");
	}

	@Override
	public ServiceProperties getService() {

		List<ServiceProperties> services = serviceDiscovery.getServices();
		return getRandomLocalService(services, preferredDatacenter);
	}
	
	/**
	 * @return A random service from the specified preferredDatacenter (if possible), 
	 * otherwise a random service from allServices.
	 */
	private ServiceProperties getRandomLocalService(final List<ServiceProperties> allServices, final String preferredDatacenter)  {
		
		final int allServicesSize = allServices.size();
		if (allServicesSize == 0) {
			LOGGER.warn("getRandomLocalService() no service found!");
			return null;
		}
		
		final List<ServiceProperties> localServices = new ArrayList<ServiceProperties>();
		
		final Iterator<ServiceProperties> i = allServices.iterator();
		while (i.hasNext()) {
			final ServiceProperties currentService = i.next();
			if (currentService.getDatacenterLocation().equals(preferredDatacenter)) {
				localServices.add(currentService);
			}
		}
		
		final int localServicesSize = localServices.size();
		if (localServicesSize == 0) {
			final int randomIndex = random.nextInt(allServicesSize);
			LOGGER.info("getRandomLocalService() no service available from datacenter={}", preferredDatacenter);
			allServices.get(randomIndex);
		}
		final int randomIndex = random.nextInt(localServicesSize);
		return localServices.get(randomIndex);
	}
}
