package eu.europeana.cloud.service.uis.config;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
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

@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice(basePackages = {"eu.europeana.cloud.service.uis.rest"})
@ApiResponses(value = {
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
    @ApiResponse(responseCode = "500", description = "Internal server error has been encountered",
        content = {
            @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorInfo.class)),
            @Content(mediaType = MediaType.APPLICATION_XML_VALUE, schema = @Schema(implementation = ErrorInfo.class))
        }),
})
public class UnifiedExceptionsMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedExceptionsMapper.class);

  private static final String OTHER_ERROR_CODE_MESSAGE = "OTHER";

  @ExceptionHandler({
      IdHasBeenMappedException.class,
      ProviderDoesNotExistException.class,
      RecordDatasetEmptyException.class,
      RecordDoesNotExistException.class,
      RecordExistsException.class,
      RecordIdDoesNotExistException.class,
      ProviderAlreadyExistsException.class,
      CloudIdAlreadyExistException.class,
      CloudIdDoesNotExistException.class
  })
  public ResponseEntity<ErrorInfo> handleException(GenericException e) {
    LOGGER.info("Exception handling fired for", e);
    return buildResponse(e);
  }

  @ExceptionHandler({
          DatabaseConnectionException.class
  })
  public ResponseEntity<ErrorInfo> handleErrorLevelException(GenericException e) {
    LOGGER.error("Exception handling fired for", e);
    return buildResponse(e);
  }

  @ExceptionHandler(AccessDeniedException.class)
  public ResponseEntity<ErrorInfo> handleAccessDeniedException(AccessDeniedException e) {
    LOGGER.info("Exception handling fired for ", e);
    return ResponseEntity
        .status(HttpStatus.METHOD_NOT_ALLOWED.value())
        .body(new ErrorInfo(OTHER_ERROR_CODE_MESSAGE, e.getMessage()));
  }

  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<ErrorInfo> handleRuntimeException(RuntimeException e) {
    LOGGER.error("Exception handling fired for ", e);
    return ResponseEntity
        .status(HttpStatus.INTERNAL_SERVER_ERROR.value())
        .body(new ErrorInfo(OTHER_ERROR_CODE_MESSAGE, e.getMessage()));
  }

  private ResponseEntity<ErrorInfo> buildResponse(GenericException e) {
    return ResponseEntity.status(e.getErrorInfo().getHttpCode().getStatusCode())
                         .body(e.getErrorInfo().getErrorInfo());
  }
}
