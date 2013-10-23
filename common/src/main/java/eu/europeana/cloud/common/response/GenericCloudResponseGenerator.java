package eu.europeana.cloud.common.response;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Helper class that generates a JSON/XML response
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 01, 2013
 */
public class GenericCloudResponseGenerator {
    /**
     * Generate a cloud response ensuring that void responses are handled correctly
     * 
     * @param statusCode
     * @param message
     * @return The JSON/XML response served by the API
     */
    public static <T> Response generateCloudResponse(CloudStatus statusCode, T message) {
        // If everything went ok and the result is not a String
        if (statusCode.getHttpCode().equals(Status.OK) &&
            !message.getClass().isAssignableFrom(String.class)) { return Response.status(Status.OK).entity(
                message).build(); }

        // For exceptions and simple OK responses
        GenericCloudResponse<T> response = new GenericCloudResponse<T>();
        response.setStatusCode(statusCode.getStatusCode());
        response.setDescription(statusCode.getDescription());
        response.setResponse(message);
        return Response.status(statusCode.getHttpCode()).entity(response).build();
    }
}
