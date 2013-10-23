package eu.europeana.cloud.common.response;

import javax.ws.rs.core.Response.Status;

/**
 * Generic Interface to be implemented by the Enumeration of status codes. This is used to expose
 * the HTTP Code and the description send to the users
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 22, 2013
 */
public class CloudStatus {
    /**
     * The application status call
     */
    private String statusCode;

    /**
     * The description of the message used for simple messages or human readable exceptions
     */
    private Status httpCode;

    /**
     * The HTTP status code
     */
    private String description;

    /**
     * Constructor of a StatusCode
     * 
     * @param statuscode
     *            The application status call
     * @param description
     *            The description of the message used for simple messages or human readable
     *            exceptions
     * @param httpCode
     *            The HTTP status code
     */
    public CloudStatus(String statuscode, String description, Status httpCode) {
        this.statusCode = statuscode;
        this.httpCode = httpCode;
        this.description = description;
    }

    /**
     * @return The application status call
     */
    public String getStatusCode() {
        return statusCode;
    }

    /**
     * @return The description of the message used for simple messages or human readable exceptions
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return The HTTP status code
     */
    public Status getHttpCode() {
        return httpCode;
    }
}
