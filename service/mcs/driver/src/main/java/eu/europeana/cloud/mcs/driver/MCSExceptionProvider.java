package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;

/**
 * Class with static method for generating Exceptions from ErrorInfo objects.
 */
public class MCSExceptionProvider {

    private MCSExceptionProvider() {
    }

    /**
     * Generate {@link MCSException} from {@link ErrorInfo}.
     * <p>
     * This method is intended to be used everywhere where we want to translate MCS error message to appropriate
     * exception and throw it. Error message handling occurs in different methods so we avoid code repetition.
     * <p>
     * Method returns the child classes of {@link MCSException}. It should not return general MCSException, unless new
     * error code was introduced in {@link McsErrorCode} Method can throw DriverException if MCS responded with HTTP 500
     * code (InternalServerError).
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
