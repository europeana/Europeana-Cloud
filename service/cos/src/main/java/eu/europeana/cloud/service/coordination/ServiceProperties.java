package eu.europeana.cloud.service.coordination;

import java.util.Random;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonRootName;

/**
 * List of properties required to connect to a Service.
 * 
 * Services can register their availability by sending those properties 
 * to a Discovery Service (currently Zookeeper). 
 * 
 * Clients can then query Zookeeper, receive the properties and choose a service to connect to
 * (and can optionally load-balance using 
 * {@link #databaseLoad} and {@link #serviceLoad}).
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
@JsonRootName("Service")
public final class ServiceProperties {

	@JsonProperty("serviceId")
	private final String serviceId;

	@JsonProperty("listenAddress")
	private String listenAddress;
	
	@JsonProperty("serviceName")
	private final String serviceName;
	
	@JsonProperty("datacenterLocation")
	private final String datacenterLocation;

	@JsonProperty("databaseLoad")
	private final int databaseLoad;
	
	@JsonProperty("serviceLoad")
	private final int serviceLoad;
	
	@JsonCreator
	public ServiceProperties(final @JsonProperty("serviceId") String serviceId,
			final @JsonProperty("serviceName") String serviceName,
			final @JsonProperty("listenAddress") String listenAddress,
			final @JsonProperty("datacenterLocation") String datacenterLocation) {
		
		this.serviceId = serviceId;
		this.listenAddress = listenAddress;
		this.serviceName = serviceName;
		this.databaseLoad = randomInteger(0, 100);
		this.serviceLoad = randomInteger(0, 100);
		this.datacenterLocation = datacenterLocation;
	}
	
	public ServiceProperties(final String serviceId,
			final String serviceName,
			final String listenAddress,
			final String datacenterLocation,
			final int databaseLoad,
			final int serviceLoad) {
		
		this.serviceId = serviceId;
		this.listenAddress = listenAddress;
		this.serviceName = serviceName;
		this.databaseLoad = databaseLoad;
		this.serviceLoad = serviceLoad;
		this.datacenterLocation = datacenterLocation;
	}
	
	/**
	 * Returns a pseudo-random number between min and max, inclusive.
	 * The difference between min and max can be at most
	 * <code>Integer.MAX_VALUE - 1</code>.
	 *
	 * @param min Minimum value
	 * @param max Maximum value.  Must be greater than min.
	 * @return Integer between min and max, inclusive.
	 * @see java.util.Random#nextInt(int)
	 */
	public static int randomInteger(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
	
	public String getDatacenterLocation() {
		return datacenterLocation;
	}

	public String getServiceId() {
		return serviceId;
	}

	public String getListenAddress() {
		return listenAddress;
	}
	
	public String getServiceName() {
		return serviceName;
	}

	public int getServiceLoad() {
		return serviceLoad;
	}
	
	public int getDatabaseLoad() {
		return databaseLoad;
	}
	
	public void setListenAddress(final String listenAddress) {
		this.listenAddress = listenAddress;
	}
	
	@Override
	public String toString() {
		
		final StringBuffer s = new StringBuffer();
		s.append("Service Name: ");
		s.append(serviceName);
		s.append(", ");
		s.append("ListenAddress: ");
		s.append(listenAddress);
		s.append(", ");
		s.append("ServiceLoad: ");
		s.append(serviceLoad);
		s.append(", ");
		s.append("DatabaseLoad: ");
		s.append(databaseLoad);
		return s.toString();
	}
}
