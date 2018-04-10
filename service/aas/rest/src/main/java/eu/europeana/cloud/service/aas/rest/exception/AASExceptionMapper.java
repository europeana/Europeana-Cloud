package eu.europeana.cloud.service.aas.rest.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.*;

import javax.ws.rs.core.Response;

/**
 * Generic class exposing the exceptions
 *
 * @author Markus.Muhr@theeuropeanlibrary.org
 * @since Aug 08, 2014
 */
public class AASExceptionMapper {

    /**
     * @param e A {@link DatabaseConnectionException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(DatabaseConnectionException e) {
        return buildResponse(e);
    }

    /**
     * @param e A {@link InvalidUsernameException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(InvalidUsernameException e) {
        return buildResponse(e);
    }

    /**
     * @param e A {@link ProviderDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(InvalidPasswordException e) {
        return buildResponse(e);
    }

    /**
     * @param e A {@link ProviderDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(UserExistsException e) {
        return buildResponse(e);
    }
    
    /**
     * @param e A {@link ProviderDoesNotExistException}
     * @return An API exception response corresponding to the exception
     */
    public Response toResponse(UserDoesNotExistException e) {
        return buildResponse(e);
    }
    

    private Response buildResponse(GenericException e) {
        return Response.status(e.getErrorInfo().getHttpCode()).entity(e.getErrorInfo().getErrorInfo()).build();
    }
}
