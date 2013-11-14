package eu.europeana.cloud.client.uis.rest;

/**
 * A checked Cloud exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class CloudException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6258948802289732378L;
	/**
	 * Initialization of the Cloud exception
	 * @param message The message to propagate to the client
	 */
	public CloudException(String message){
		super(message);
	}
}
