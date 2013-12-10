package eu.europeana.cloud.client.uis.rest;

import eu.europeana.cloud.service.uis.exception.GenericException;

public class  CloudException  extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8451384934113123019L;

	public <T extends GenericException> CloudException(String message,T  t) {
		super(message,t);
	}

}
