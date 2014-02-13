package eu.europeana.cloud.service.mcs.status;

import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Status codes used to as an error code in {@link ErrorInfo}.
 */
public enum McsErrorCode {

    CANNOT_MODIFY_PERSISTENT_REPRESENTATION,
    DATASET_ALREADY_EXISTS,
    DATASET_NOT_EXISTS,
    FILE_ALREADY_EXISTS,
    FILE_NOT_EXISTS,
    PROVIDER_NOT_EXISTS,
    RECORD_NOT_EXISTS,
    REPRESENTATION_NOT_EXISTS,
    VERSION_NOT_EXISTS,
    FILE_CONTENT_HASH_MISMATCH,
    REPRESENTATION_ALREADY_IN_SET,
    CANNOT_PERSIST_EMPTY_REPRESENTATION,
    WRONG_CONTENT_RANGE,
    OTHER

}
