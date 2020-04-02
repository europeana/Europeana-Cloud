package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.ws.rs.WebApplicationException;


/**
 * Maps exceptions thrown by services to {@link ResponseEntity}.
 */
@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice(basePackages={"eu.europeana.cloud.service.mcs.rest"})
public class UnitedExceptionMapper {

    private final static Logger LOGGER = LoggerFactory.getLogger(UnitedExceptionMapper.class);

    /**
     * Maps {@link CannotModifyPersistentRepresentationException} to {@link ResponseEntity}. Returns a response with HTTP
     * status code 405 - "Method Not Allowed" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    @ExceptionHandler(CannotModifyPersistentRepresentationException.class)
    @ResponseBody public ErrorInfo handleCannotModifyPersistentRepresentationException(
            CannotModifyPersistentRepresentationException exception) {
        return buildResponse(McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION, exception);
    }


    /**
     * Maps {@link CannotPersistEmptyRepresentationException} to {@link ResponseEntity}. Returns a response with HTTP status
     * code 405 - "Method Not Allowed" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    @ExceptionHandler(CannotPersistEmptyRepresentationException.class)
    @ResponseBody public ErrorInfo handleCannotPersistEmptyRepresentationException(Exception exception) {
        return buildResponse(McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION, exception);
    }


    /**
     * Maps {@link DataSetAlreadyExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 409 -
     * "Conflict" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.CONFLICT)
    @ExceptionHandler(DataSetAlreadyExistsException.class)
    @ResponseBody public ErrorInfo handleDataSetAlreadyExistsException(Exception exception) {
        return buildResponse(McsErrorCode.DATASET_ALREADY_EXISTS, exception);
    }


    /**
     * Maps {@link DataSetNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(DataSetNotExistsException.class)
    @ResponseBody public ErrorInfo handleDataSetNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.DATASET_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link FileAlreadyExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 409 -
     * "Conflict" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.CONFLICT)
    @ExceptionHandler(FileAlreadyExistsException.class)
    @ResponseBody public ErrorInfo handleFileAlreadyExistsException(Exception exception) {
        return buildResponse(McsErrorCode.FILE_ALREADY_EXISTS, exception);
    }


    /**
     * Maps {@link FileNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(FileNotExistsException.class)
    @ResponseBody public ErrorInfo handleFileNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.FILE_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link RecordNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(RecordNotExistsException.class)
    @ResponseBody public ErrorInfo handleRecordNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.RECORD_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link RepresentationNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(RepresentationNotExistsException.class)
    @ResponseBody public ErrorInfo handleRepresentationNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.REPRESENTATION_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link VersionNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(VersionNotExistsException.class)
    @ResponseBody public ErrorInfo handleVersionNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.VERSION_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link FileContentHashMismatchException} to {@link ResponseEntity}. Returns a response with HTTP status code 422 -
     * "Unprocessable Entity" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    @ExceptionHandler(FileContentHashMismatchException.class)
    @ResponseBody public ErrorInfo handleFileContentHashMismatchException(Exception exception) {
        return buildResponse(McsErrorCode.FILE_CONTENT_HASH_MISMATCH, exception);
    }


    /**
     * Maps {@link WrongContentRangeException} to {@link ResponseEntity}. Returns a response with HTTP status code 416 -
     * "Requested Range Not Satisfiable" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
    @ExceptionHandler(WrongContentRangeException.class)
    @ResponseBody public ErrorInfo handleWrongContentRangeException(Exception exception) {
        return buildResponse(McsErrorCode.WRONG_CONTENT_RANGE, exception);
    }


    /**
     * Maps {@link RuntimeException} to {@link ResponseEntity}. Returns a response with HTTP status code 500 -
     * "Internal Server Error" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ErrorInfo> toResponse(RuntimeException exception) {

        if (exception instanceof AccessDeniedException) {
            return buildResponse(HttpStatus.METHOD_NOT_ALLOWED,
                    McsErrorCode.ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION,
                    exception);
        }

        if (exception instanceof IllegalArgumentException) {
            return buildResponse(HttpStatus.BAD_REQUEST, McsErrorCode.BAD_PARAMETER_VALUE, exception);
        }

        LOGGER.error("Unexpected error occured.", exception);
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, McsErrorCode.OTHER, exception);
    }


    /**
     * Maps {@link WebApplicationException} to {@link ResponseEntity}. Returns a response with from a given exception and a
     * {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(WebApplicationException.class)
    public ResponseEntity<ErrorInfo> toResponse(WebApplicationException exception) {
        return buildResponse(
                HttpStatus.valueOf(exception.getResponse().getStatus()),
                McsErrorCode.OTHER, exception);
    }

    /**
     * Maps {@link ProviderNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not Found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(ProviderNotExistsException.class)
    @ResponseBody public ErrorInfo handleProviderNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.PROVIDER_NOT_EXISTS, exception);
    }


    /**
     * Maps {@link AccessDeniedOrObjectDoesNotExistException} to {@link ResponseEntity}. Returns a response with HTTP status code 403 -
     * "Method not Allowed" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    @ExceptionHandler(AccessDeniedOrObjectDoesNotExistException.class)
    @ResponseBody public ErrorInfo handleAccessDeniedOrObjectDoesNotExistException(Exception exception) {
        return buildResponse(McsErrorCode.ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION, exception);
    }


    /**
     * Maps {@link RevisionIsNotValidException} to {@link ResponseEntity}. Returns a response with HTTP status code 405 -
     * "Method not Allowed" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    @ExceptionHandler(RevisionIsNotValidException.class)
    @ResponseBody public ErrorInfo handleRevisionIsNotValidException(Exception exception) {
        return buildResponse(McsErrorCode.REVISION_IS_NOT_VALID, exception);
    }


    /**
     * Maps {@link RevisionNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 -
     * "Not found" and a {@link ErrorInfo} with exception details as a message body.
     *
     * @param exception the exception to map to a response
     * @return a response mapped from the supplied exception
     */
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(RevisionNotExistsException.class)
    @ResponseBody public ErrorInfo handleRevisionNotExistsException(Exception exception) {
        return buildResponse(McsErrorCode.REVISION_NOT_EXISTS, exception);
    }


/*
    private static Response buildResponse(Response.Status httpStatus, McsErrorCode errorCode, Exception e) {
        return buildResponse(httpStatus.getStatusCode(), errorCode, e);
    }
*/


    private static ResponseEntity<ErrorInfo> buildResponse(HttpStatus httpStatusCode, McsErrorCode errorCode, Exception e) {
        return ResponseEntity
                .status(httpStatusCode)
                .contentType(MediaType.APPLICATION_XML)
                .body(new ErrorInfo(errorCode.name(), e.getMessage()));
    }

    private static ErrorInfo buildResponse(McsErrorCode errorCode, Exception e) {
        return buildResponse(errorCode, e.getMessage());
    }

    private static ErrorInfo buildResponse(McsErrorCode errorCode, String message) {
        return new ErrorInfo(errorCode.name(), message);
    }

}
