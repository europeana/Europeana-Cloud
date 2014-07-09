package eu.europeana.cloud.service.coordination.discovery;

import java.util.List;

import eu.europeana.cloud.service.coordination.ServiceProperties;

/**
 * A mechanism to ask for ecloud's service instances. 
 *  
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public interface EcloudServiceDiscovery {

	List<ServiceProperties> getServices();
}
