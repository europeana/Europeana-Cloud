package eu.europeana.cloud.client.uis.rest;

import eu.europeana.cloud.service.uis.exception.GenericException;

/**
 * Generic Cloud Exception
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class  CloudException  extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8451384934113123019L;

	/**
	 * Creates a new instance of this class.
	 * @param message
	 * @param t The cloud exception to wrap
	 */
	public <T extends GenericException> CloudException(String message,T  t) {
		super(message,t);
	}

}
