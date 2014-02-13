package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSet;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;

public class MCSExceptionProvider {

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
            case VERSION_NOT_EXISTS:
                return new VersionNotExistsException(details);
            case FILE_CONTENT_HASH_MISMATCH:
                return new FileContentHashMismatchException(details);
            case REPRESENTATION_ALREADY_IN_SET:
                return new RepresentationAlreadyInSet(details);
            case CANNOT_PERSIST_EMPTY_REPRESENTATION:
                return new CannotPersistEmptyRepresentationException(details);
            case WRONG_CONTENT_RANGE:
                return new WrongContentRangeException(details);     
            case OTHER:
                throw new DriverException(details); //this error means always InternalServerError TODO move to JavaDoc
            default:
                return new MCSException(details); //this will happen only if somebody uses code newly introdued to MscErrorCode        
        }

    }
}
