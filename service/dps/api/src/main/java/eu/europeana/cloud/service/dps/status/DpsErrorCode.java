package eu.europeana.cloud.service.dps.status;

import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Status codes used to as an error code in {@link ErrorInfo}.
 */
public enum DpsErrorCode {

	ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION,
    ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION,
    TOPOLOGY_ALREADY_EXIST,
    BAD_REQUEST,
    TASK_NOT_VALID,
    DATABASE_CONNECTION_EXCEPTION,
    TASKINFO_DOES_NOT_EXIST_EXCEPTION,
    OTHER

}
