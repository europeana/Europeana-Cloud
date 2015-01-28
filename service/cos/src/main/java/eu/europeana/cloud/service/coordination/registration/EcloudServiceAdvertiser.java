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
	
	/**
	 * @return Id of the currently advertised service.
	 * Auto-generated.
	 * 
	 * Example: "1b96c813-0ec2-4038-ab49-1ef6a1a73083"
	 */
	String getCurrentlyAdvertisedServiceID();
	
	/**
	 * @return Address of the currently advertised service.
	 * 
	 * Example: "http://146.48.82.158:8080/ecloud-service-uis-rest-0.3-SNAPSHOT"
	 */
	String getCurrentlyAdvertisedServiceAddress();
}
