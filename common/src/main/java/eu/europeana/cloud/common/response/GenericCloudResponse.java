package eu.europeana.cloud.common.response;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A generic response wrapper
 * 
 * @param <T>
 *            Class agnostic response wrapper
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 01, 2013
 */
@XmlRootElement
public class GenericCloudResponse<T> {
    /**
     * The confirmation or error message of the response
     */
    private String statusCode;

    /**
     * A human readable description for simple responses
     */
    private String description;

    /**
     * The JSON/XML response
     */
    private T      response;

    /**
     * @return A human readable description for simple responses
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description
     *            A human readable description for simple responses
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return The confirmation or error message of the response
     */
    public String getStatusCode() {
        return this.statusCode;
    }

    /**
     * @param statusCode
     *            The confirmation or error message of the response
     */
    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * @return The JSON/XML response
     */
    public T getResponse() {
        return response;
    }

    /**
     * @param response
     *            The JSON/XML response
     */
    public void setResponse(T response) {
        this.response = response;
    }
}
