package eu.europeana.cloud.service.coordination.discovery;

import eu.europeana.cloud.service.coordination.ServiceProperties;

import java.util.List;

/**
 * A mechanism to ask for ecloud's service instances. 
 *  
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public interface EcloudServiceDiscovery {

	List<ServiceProperties> getServices();
}
