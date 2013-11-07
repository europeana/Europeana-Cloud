package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;

/**
 * McsErrorCode
 */
public enum McsErrorCode {

    CANNOT_MODIFY_PERSISTENT_REPRESENTATION,
    DATASET_ALREADY_EXISTS,
    DATASET_NOT_EXISTS,
    FILE_ALREADY_EXISTS,
    FILE_NOT_EXISTS,
    PROVIDER_HAS_DATASETS,
    PROVIDER_HAS_RECORDS,
    PROVIDER_NOT_EXISTS,
    RECORD_NOT_EXISTS,
    REPRESENTATION_NOT_EXISTS,
    VERSION_NOT_EXISTS,
    FILE_CONTENT_HASH_MISMATCH,
    REPRESENTATION_ALREADY_IN_SET,
    OTHER

}
