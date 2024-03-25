package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSet;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.core.Response;

/**
 * Class with static method for generating Exceptions from ErrorInfo objects.
 */
public final class MCSExceptionProvider {

  private MCSExceptionProvider() {
  }


  public static MCSException generateException(Response response) {

    try {
      response.bufferEntity();
      ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
      return MCSExceptionProvider.generateException(errorInfo);
    } catch (ProcessingException e) {
      String message = response.readEntity(String.class);
      throw new RuntimeException("Cound not deserialize response with statusCode: " + response.getStatus()
          + ", and message: " + message, e);
    }
  }


  /**
   * Generate {@link MCSException} from {@link ErrorInfo}.
   * <p>
   * This method is intended to be used everywhere where we want to translate MCS error message to appropriate exception and throw
   * it. Error message handling occurs in different methods so we avoid code repetition.
   * <p>
   * Method returns the child classes of {@link MCSException}. It should not return general MCSException, unless new error code
   * was introduced in {@link McsErrorCode} Method can throw DriverException if MCS responded with HTTP 500 code
   * (InternalServerError).
   *
   * @param errorInfo object storing error information returned by MCS
   * @return MCSException to be thrown
   */
  public static MCSException generateException(ErrorInfo errorInfo) {

    if (errorInfo == null) {
      throw new DriverException("Null errorInfo passed to generating exception.");
    }

    McsErrorCode errorCode;
    try {
      errorCode = McsErrorCode.valueOf(errorInfo.getErrorCode());
    } catch (IllegalArgumentException e) {
      throw new DriverException("Unknown errorCode returned from service.", e);
    }

    String details = errorInfo.getDetails();

    switch (errorCode) {
      case ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION:
        return new AccessDeniedOrObjectDoesNotExistException(details);
      case CANNOT_MODIFY_PERSISTENT_REPRESENTATION:
        return new CannotModifyPersistentRepresentationException(details);
      case DATASET_ALREADY_EXISTS:
        return new DataSetAlreadyExistsException(details);
      case DATASET_NOT_EXISTS:
        return new DataSetNotExistsException(details);
      case FILE_ALREADY_EXISTS:
        return new FileAlreadyExistsException(details);
      case FILE_NOT_EXISTS:
        return new FileNotExistsException(details);
      case PROVIDER_NOT_EXISTS:
        return new ProviderNotExistsException(details);
      case RECORD_NOT_EXISTS:
        return new RecordNotExistsException(details);
      case REPRESENTATION_NOT_EXISTS:
        return new RepresentationNotExistsException(details);
      case FILE_CONTENT_HASH_MISMATCH:
        return new FileContentHashMismatchException(details);
      case REPRESENTATION_ALREADY_IN_SET:
        return new RepresentationAlreadyInSet(details);
      case CANNOT_PERSIST_EMPTY_REPRESENTATION:
        return new CannotPersistEmptyRepresentationException(details);
      case WRONG_CONTENT_RANGE:
        return new WrongContentRangeException(details);
      case OTHER:
        throw new DriverException(details);
      default:
        return new MCSException(details); //this will happen only if somebody uses code newly introdued to MscErrorCode
    }
  }

  public static MCSException createException(String message, Throwable throwable) {
    return new MCSException(message, throwable);
  }
}
