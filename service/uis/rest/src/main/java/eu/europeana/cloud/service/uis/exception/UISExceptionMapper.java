package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.security.access.AccessDeniedException;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Generic class exposing the exceptions
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class UISExceptionMapper {

    /**
     * @param e
     *            A {@link CloudIdDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(CloudIdDoesNotExistException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link DatabaseConnectionException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(DatabaseConnectionException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            An {@link IdHasBeenMappedException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(IdHasBeenMappedException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link ProviderDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(ProviderDoesNotExistException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link RecordDatasetEmptyException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(RecordDatasetEmptyException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link RecordDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(RecordDoesNotExistException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link RecordExistsException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(RecordExistsException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link RecordIdDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(RecordIdDoesNotExistException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link ProviderAlreadyExistsException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(ProviderAlreadyExistsException e) {
	return buildResponse(e);
    }

    /**
     * @param e
     *            A {@link javax.ws.rs.WebApplicationException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(WebApplicationException e) {
	return Response.status(Response.Status.NOT_FOUND)
		.type(MediaType.APPLICATION_XML)
		.entity(new ErrorInfo("OTHER", e.getMessage())).build();
    }

    public Response toResponse(RuntimeException e) {

	if (e instanceof AccessDeniedException) {
	    return Response.status(Response.Status.METHOD_NOT_ALLOWED)
		    .type(MediaType.APPLICATION_XML)
		    .entity(new ErrorInfo("OTHER", e.getMessage())).build();
	}
	return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
		.type(MediaType.APPLICATION_XML)
		.entity(new ErrorInfo("OTHER", e.getMessage())).build();
    }

    private Response buildResponse(GenericException e) {
	return Response.status(e.getErrorInfo().getHttpCode())
		.entity(e.getErrorInfo().getErrorInfo()).build();
    }

    /**
     * @param e
     *            A {@link CloudIdAlreadyExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(CloudIdAlreadyExistException e) {
	return buildResponse(e);
    }
}
