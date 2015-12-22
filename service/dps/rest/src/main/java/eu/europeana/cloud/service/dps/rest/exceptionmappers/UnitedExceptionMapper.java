package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.TopologyAlreadyExistsException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.status.DpsErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Maps exceptions thrown by services to {@link Response}.
 */
public class UnitedExceptionMapper {

    static final int UNPROCESSABLE_ENTITY = 422;

    private final static Logger LOGGER = LoggerFactory.getLogger(UnitedExceptionMapper.class);

    /**
     * Maps {@link CannotModifyPersistentRepresentationException} to {@link Response}. Returns a response with HTTP
     * status code 405 - "Method Not Allowed" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(TopologyAlreadyExistsException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, DpsErrorCode.TOPOLOGY_ALREADY_EXIST,
            exception);
    }


    /**
     * Maps {@link RuntimeException} to {@link Response}. Returns a response with HTTP status code 500 -
     * "Internal Server Error" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(RuntimeException exception) {
    	
    	if (exception instanceof AccessDeniedException) {
            return buildResponse(Response.Status.METHOD_NOT_ALLOWED, 
            		DpsErrorCode.ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION,
                exception);
    	}

        LOGGER.error("Unexpected error occured.", exception);
        return buildResponse(Response.Status.INTERNAL_SERVER_ERROR, DpsErrorCode.OTHER, exception);
    }


    /**
     * Maps {@link WebApplicationException} to {@link Response}. Returns a response with from a given exception and a
     * {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(WebApplicationException exception) {
        return buildResponse(exception.getResponse().getStatus(), DpsErrorCode.OTHER, exception);
    }

    /**
     * Maps {@link AccessDeniedOrTopologyDoesNotExistException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(AccessDeniedOrTopologyDoesNotExistException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, DpsErrorCode.ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION, exception);
    }

    public Response toResponse(DpsTaskValidationException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, DpsErrorCode.TASK_NOT_VALID, exception);
    }

    public Response toResponse(TaskSubmissionException exception) {
        return buildResponse(Response.Status.BAD_REQUEST, DpsErrorCode.TASK_NOT_VALID, exception);
    }
    
    private static Response buildResponse(Response.Status httpStatus, DpsErrorCode errorCode, Exception e) {
        return buildResponse(httpStatus.getStatusCode(), errorCode, e);
    }


    private static Response buildResponse(int httpStatusCode, DpsErrorCode errorCode, Exception e) {
        return Response.status(httpStatusCode).type(MediaType.APPLICATION_XML).entity(new ErrorInfo(errorCode.name(), e.getMessage())).build();
    }
}
