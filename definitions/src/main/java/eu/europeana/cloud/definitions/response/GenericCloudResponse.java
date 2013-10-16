package eu.europeana.cloud.definitions.response;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.definitions.StatusCode;

/**
 * A generic response wrapper
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 * @param <T>
 *            Class agnostic response wrapper
 */
@XmlRootElement
public class GenericCloudResponse<T> {

	/**
	 * The confirmation or error message of the response
	 */
	StatusCode statusCode;
	
	/**
	 * The JSON/XML response
	 */
	T response;

	public StatusCode getStatusCode() {
		return this.statusCode;
	}

	public void setStatusCode(StatusCode statusCode) {
		this.statusCode = statusCode;
	}

	public T getResponse() {
		return response;
	}

	public void setResponse(T response) {
		this.response = response;
	}
}
