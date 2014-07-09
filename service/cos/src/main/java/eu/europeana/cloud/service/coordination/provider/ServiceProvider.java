package eu.europeana.cloud.service.coordination.provider;

import eu.europeana.cloud.service.coordination.ServiceProperties;

/**
 * Main API for Service Discovery. 
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public interface ServiceProvider {

	ServiceProperties getService();
}
