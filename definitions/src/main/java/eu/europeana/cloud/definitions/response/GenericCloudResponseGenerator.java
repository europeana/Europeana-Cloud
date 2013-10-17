package eu.europeana.cloud.definitions.response;

import javax.ws.rs.core.Response;

import eu.europeana.cloud.definitions.StatusCode;

/**
 * Helper class that generates a JSON/XML response
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class GenericCloudResponseGenerator {

	/**
	 * Generate a cloud response ensuring that void responses are handled correctly
	 * @param statusCode
	 * @param message
	 * @return The JSON/XML response served by the API
	 */
	public static <T> Response generateCloudResponse(StatusCode statusCode, T message) {
		if (statusCode.equals(StatusCode.OK)
				&& !message.getClass().isAssignableFrom(String.class)) {
			return Response.status(statusCode.getHttpCode()).entity(message)
					.build();
		}
		GenericCloudResponse<T> response = new GenericCloudResponse<T>();
		response.setStatusCode(statusCode);
		response.setResponse(message);
		return Response.status(statusCode.getHttpCode()).entity(response)
				.build();

	}
}
