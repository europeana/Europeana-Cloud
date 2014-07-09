package eu.europeana.cloud.service.coordination.registration;

import eu.europeana.cloud.service.coordination.ServiceProperties;

/**
 * A mechanism to advertise ecloud's service instances. 
 *  
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public interface EcloudServiceAdvertiser {

	/**
	 * Starts advertising the specified service.
	 * 
	 * @param serviceProperties List of properties required to connect to this Service.
	 */
	void startAdvertising(ServiceProperties serviceProperties);

	/**
	 * Stops advertising a currently advertised service.
	 */
	void stopAdvertising();
}
