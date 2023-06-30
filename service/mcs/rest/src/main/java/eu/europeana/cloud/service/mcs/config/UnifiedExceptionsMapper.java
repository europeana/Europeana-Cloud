package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.DataSetDeletionException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 * Maps exceptions thrown by services to {@link ResponseEntity}.
 */
@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice(basePackages = {"eu.europeana.cloud.service.mcs.controller"})
@ApiResponses(value = {
    @ApiResponse(responseCode = "400", description = "Bad request",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
    @ApiResponse(responseCode = "403", description = "Access has been denied",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
    @ApiResponse(responseCode = "404", description = "Resource has not been found",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
    @ApiResponse(responseCode = "405", description = "Resource doesn't exist or access has been denied",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
    @ApiResponse(responseCode = "409", description = "Conflict has been encountered",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
        }),
    @ApiResponse(responseCode = "416", description = "Requested range is not satisfiable",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
    @ApiResponse(responseCode = "422", description = "Unprocessable entity has been encountered",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
    @ApiResponse(responseCode = "500", description = "Internal server error has been encountered",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
})
public class UnifiedExceptionsMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedExceptionsMapper.class);

  private final HttpServletRequest request;

  public UnifiedExceptionsMapper(HttpServletRequest request) {
    this.request = request;
  }

  /**
   * Maps {@link CannotModifyPersistentRepresentationException} to {@link ResponseEntity}. Returns a response with HTTP status
   * code 405 - "Method Not Allowed" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(CannotModifyPersistentRepresentationException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleCannotModifyPersistentRepresentationException(
      CannotModifyPersistentRepresentationException exception) {
    return buildResponse(HttpStatus.METHOD_NOT_ALLOWED, McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION, exception);
  }


  /**
   * Maps {@link CannotPersistEmptyRepresentationException} to {@link ResponseEntity}. Returns a response with HTTP status code
   * 405 - "Method Not Allowed" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(CannotPersistEmptyRepresentationException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleCannotPersistEmptyRepresentationException(Exception exception) {
    return buildResponse(HttpStatus.METHOD_NOT_ALLOWED, McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION, exception);
  }


  /**
   * Maps {@link DataSetAlreadyExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 409 -
   * "Conflict" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(DataSetAlreadyExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleDataSetAlreadyExistsException(Exception exception) {
    return buildResponse(HttpStatus.CONFLICT, McsErrorCode.DATASET_ALREADY_EXISTS, exception);
  }


  /**
   * Maps {@link DataSetNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 - "Not Found"
   * and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(DataSetNotExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleDataSetNotExistsException(Exception exception) {
    return buildResponse(HttpStatus.NOT_FOUND, McsErrorCode.DATASET_NOT_EXISTS, exception);
  }


  /**
   * Maps {@link FileAlreadyExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 409 - "Conflict"
   * and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(FileAlreadyExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleFileAlreadyExistsException(Exception exception) {
    return buildResponse(HttpStatus.CONFLICT, McsErrorCode.FILE_ALREADY_EXISTS, exception);
  }


  /**
   * Maps {@link FileNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 - "Not Found" and
   * a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(FileNotExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleFileNotExistsException(Exception exception) {
    return buildResponse(HttpStatus.NOT_FOUND, McsErrorCode.FILE_NOT_EXISTS, exception);
  }

  /**
   * Maps {@link RecordNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 - "Not Found"
   * and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(RecordNotExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleRecordNotExistsException(Exception exception) {
    return buildResponse(HttpStatus.NOT_FOUND, McsErrorCode.RECORD_NOT_EXISTS, exception);
  }


  /**
   * Maps {@link RepresentationNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 - "Not
   * Found" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(RepresentationNotExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleRepresentationNotExistsException(Exception exception) {
    return buildResponse(HttpStatus.NOT_FOUND, McsErrorCode.REPRESENTATION_NOT_EXISTS, exception);
  }


  /**
   * Maps {@link FileContentHashMismatchException} to {@link ResponseEntity}. Returns a response with HTTP status code 422 -
   * "Unprocessable Entity" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(FileContentHashMismatchException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleFileContentHashMismatchException(Exception exception) {
    return buildResponse(HttpStatus.UNPROCESSABLE_ENTITY, McsErrorCode.FILE_CONTENT_HASH_MISMATCH, exception);
  }


  /**
   * Maps {@link WrongContentRangeException} to {@link ResponseEntity}. Returns a response with HTTP status code 416 - "Requested
   * Range Not Satisfiable" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(WrongContentRangeException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleWrongContentRangeException(Exception exception) {
    return buildResponse(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE, McsErrorCode.WRONG_CONTENT_RANGE, exception);
  }


  /**
   * Maps {@link RuntimeException} to {@link ResponseEntity}. Returns a response with HTTP status code 500 - "Internal Server
   * Error" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
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
   * {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(WebApplicationException.class)
  public ResponseEntity<ErrorInfo> toResponse(WebApplicationException exception) {
    return buildResponse(
        HttpStatus.valueOf(exception.getResponse().getStatus()),
        McsErrorCode.OTHER, exception);
  }

  /**
   * Maps {@link ProviderNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 - "Not Found"
   * and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(ProviderNotExistsException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleProviderNotExistsException(Exception exception) {
    return buildResponse(HttpStatus.NOT_FOUND, McsErrorCode.PROVIDER_NOT_EXISTS, exception);
  }


  /**
   * Maps {@link AccessDeniedOrObjectDoesNotExistException} to {@link ResponseEntity}. Returns a response with HTTP status code
   * 403 - "Method not Allowed" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(AccessDeniedOrObjectDoesNotExistException.class)
  @ResponseBody
  public ResponseEntity<ErrorInfo> handleAccessDeniedOrObjectDoesNotExistException(Exception exception) {
    return buildResponse(HttpStatus.METHOD_NOT_ALLOWED, McsErrorCode.ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION, exception);
  }


  /**
   * Maps {@link RevisionIsNotValidException} to {@link ResponseEntity}. Returns a response with HTTP status code 405 - "Method
   * not Allowed" and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(RevisionIsNotValidException.class)
  public @ResponseBody
  ResponseEntity<ErrorInfo> handleRevisionIsNotValidException(Exception exception) {
    return buildResponse(HttpStatus.METHOD_NOT_ALLOWED, McsErrorCode.REVISION_IS_NOT_VALID, exception);
  }


  /**
   * Maps {@link RevisionNotExistsException} to {@link ResponseEntity}. Returns a response with HTTP status code 404 - "Not found"
   * and a {@link ResponseEntity<ErrorInfo>} with exception details as a message body.
   *
   * @param exception the exception to map to a response
   * @return a response mapped from the supplied exception
   */
  @ExceptionHandler(RevisionNotExistsException.class)
  public @ResponseBody
  ResponseEntity<ErrorInfo> handleRevisionNotExistsException(Exception exception) {
    return buildResponse(HttpStatus.NOT_FOUND, McsErrorCode.REVISION_NOT_EXISTS, exception);
  }

  @ExceptionHandler(DataSetDeletionException.class)
  public @ResponseBody
  ResponseEntity<ErrorInfo> handleDataSetDeletionException(DataSetDeletionException exception) {
    return buildResponse(HttpStatus.BAD_REQUEST, McsErrorCode.DATASET_NOT_EMPTY, exception);
  }

  @ExceptionHandler(DataSetAssignmentException.class)
  public @ResponseBody
  ResponseEntity<ErrorInfo> handleDataSetAssignmentException(DataSetAssignmentException exception) {
    return buildResponse(HttpStatus.BAD_REQUEST, McsErrorCode.DATASET_ASSIGNMENT_MISMATCH, exception);
  }

  /**
   * Method below is instead {@link eu.europeana.cloud.service.mcs.utils.ParamUtil#require(String, Object)}
   */
  @ExceptionHandler(MissingServletRequestParameterException.class)
  public @ResponseBody
  ResponseEntity<ErrorInfo> handleMissingServletRequestParameterException(MissingServletRequestParameterException exception) {
    return buildResponse(HttpStatus.BAD_REQUEST, McsErrorCode.OTHER, exception.getParameterName() + " is a required parameter");
  }

  private ResponseEntity<ErrorInfo> buildResponse(HttpStatus status, McsErrorCode errorCode, Exception e) {
    return buildResponse(status, errorCode, e.getMessage());
  }

  private ResponseEntity<ErrorInfo> buildResponse(HttpStatus status, McsErrorCode errorCode, String message) {
    ErrorInfo errorInfo = new ErrorInfo(errorCode.name(), message);
    return ResponseEntity
        .status(status)
        .contentType(evaluateContentType())
        .body(errorInfo);
  }

  private MediaType evaluateContentType() {
    if (MediaType.APPLICATION_JSON_VALUE.equals(request.getHeader(HttpHeaders.ACCEPT))) {
      return MediaType.APPLICATION_JSON;
    } else {
      return MediaType.APPLICATION_XML;
    }
  }

}
