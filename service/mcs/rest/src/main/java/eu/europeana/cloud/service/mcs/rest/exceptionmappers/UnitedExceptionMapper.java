package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public Response toResponse(CannotModifyPersistentRepresentationException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION,
            exception);
    }


    /**
     * Maps {@link CannotPersistEmptyRepresentationException} to {@link Response}. Returns a response with HTTP status
     * code 405 - "Method Not Allowed" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(CannotPersistEmptyRepresentationException exception) {
        return buildResponse(Response.Status.METHOD_NOT_ALLOWED, McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION,
            exception);
    }


    /**
     * Maps {@link DataSetAlreadyExistsException} to {@link Response}. Returns a response with HTTP status code 409 -
     * "Conflict" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(DataSetAlreadyExistsException exception) {
        return buildResponse(Response.Status.CONFLICT, McsErrorCode.DATASET_ALREADY_EXISTS, exception);
    }


    /**
     * Maps {@link DataSetNotExistsException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(DataSetNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.DATASET_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link FileAlreadyExistsException} to {@link Response}. Returns a response with HTTP status code 409 -
     * "Conflict" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(FileAlreadyExistsException exception) {
        return buildResponse(Response.Status.CONFLICT, McsErrorCode.FILE_ALREADY_EXISTS, exception);
    }


    /**
     * Maps {@link FileNotExistsException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(FileNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.FILE_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link RecordNotExistsException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(RecordNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.RECORD_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link RepresentationNotExistsException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(RepresentationNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.REPRESENTATION_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link VersionNotExistsException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(VersionNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.VERSION_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link FileContentHashMismatchException} to {@link Response}. Returns a response with HTTP status code 422 -
     * "Unprocessable Entity" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(FileContentHashMismatchException exception) {
        return buildResponse(UNPROCESSABLE_ENTITY, McsErrorCode.FILE_CONTENT_HASH_MISMATCH, exception);
    }


    /**
     * Maps {@link WrongContentRangeException} to {@link Response}. Returns a response with HTTP status code 416 -
     * "Requested Range Not Satisfiable" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(WrongContentRangeException exception) {
        return buildResponse(Response.Status.REQUESTED_RANGE_NOT_SATISFIABLE, McsErrorCode.WRONG_CONTENT_RANGE,
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
        LOGGER.error("Unexpected error occured.", exception);
        return buildResponse(Response.Status.INTERNAL_SERVER_ERROR, McsErrorCode.OTHER, exception);
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
        return buildResponse(exception.getResponse().getStatus(), McsErrorCode.OTHER, exception);
    }


    /**
     * Maps {@link ProviderNotExistsException} to {@link Response}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     * 
     * @param exception
     *            the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    public Response toResponse(ProviderNotExistsException exception) {
        return buildResponse(Response.Status.NOT_FOUND, McsErrorCode.PROVIDER_NOT_EXISTS, exception);
    }


    private static Response buildResponse(Response.Status httpStatus, McsErrorCode errorCode, Exception e) {
        return buildResponse(httpStatus.getStatusCode(), errorCode, e);
    }


    private static Response buildResponse(int httpStatusCode, McsErrorCode errorCode, Exception e) {
        return Response.status(httpStatusCode).entity(new ErrorInfo(errorCode.name(), e.getMessage())).build();
    }
}
