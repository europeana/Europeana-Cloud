package eu.europeana.cloud.common.response;

import javax.xml.bind.annotation.XmlRootElement;

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
	String statusCode;

	/**
	 * The JSON/XML response
	 */
	T response;

	/**
	 * A human readable description for simple responses
	 */
	String description;
	
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getStatusCode() {
		return this.statusCode;
	}

	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}

	public T getResponse() {
		return response;
	}

	public void setResponse(T response) {
		this.response = response;
	}
}
