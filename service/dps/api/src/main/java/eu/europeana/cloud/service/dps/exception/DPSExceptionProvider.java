package eu.europeana.cloud.service.dps.exception;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.dps.status.DpsErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 2/6/2018.
 */
public final class DPSExceptionProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DPSExceptionProvider.class);

  private DPSExceptionProvider() {
  }

  /**
   * Create (not throw so not "generate") {@link DpsException} from {@link ErrorInfo}.
   * <p>
   * This method is intended to be used everywhere where we want to translate DPS error message to appropriate exception and throw
   * it. Error message handling occurs in different methods so we avoid code repetition.
   * <p>
   * Method returns the child classes of {@link DpsException}. It should not return general DPSException, unless new error code
   * was introduced in {@link DpsException} Method can throw DPSClientException if DPS responded with HTTP 500 code
   * (InternalServerError).
   *
   * @param errorInfo object storing error information returned by MCS
   * @return DPSException to be thrown
   */
  public static DpsException createException(ErrorInfo errorInfo) {
    LOGGER.error("errorInfo = {}", errorInfo);

    if (errorInfo == null) {
      throw new DPSClientException("Null errorInfo passed to generating exception.");
    }

    DpsErrorCode dpsErrorCode;
    try {
      dpsErrorCode = DpsErrorCode.valueOf(errorInfo.getErrorCode());
    } catch (IllegalArgumentException e) {
      throw new DPSClientException("Unknown errorCode returned from service.", e);
    }

    String details = errorInfo.getDetails();

    switch (dpsErrorCode) {
      case ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION:
        return new AccessDeniedOrObjectDoesNotExistException(details);
      case ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION:
        return new AccessDeniedOrTopologyDoesNotExistException(details);
      case TOPOLOGY_ALREADY_EXIST:
        return new TopologyAlreadyExistsException(details);
      case TASK_NOT_VALID:
        return new DpsTaskValidationException(details);
      case DATABASE_CONNECTION_EXCEPTION:
        return new DatabaseConnectionException(details);
      case TASKINFO_DOES_NOT_EXIST_EXCEPTION:
        return new TaskInfoDoesNotExistException(details);
      case OTHER:
        throw new DPSClientException(details);
      default:
        return new DpsException(details); //this will happen only if somebody uses code newly introdued to MscErrorCode
    }
  }

  public static DpsException createException(String generalMessage, String particularMessage, Throwable cause) {
    String message = String.format("%s (%s)", generalMessage, particularMessage);
    LOGGER.error(message, cause);
    return new DpsException(message, cause);
  }
}

