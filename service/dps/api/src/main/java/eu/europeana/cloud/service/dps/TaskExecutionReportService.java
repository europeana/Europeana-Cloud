package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;

import java.util.List;


/**
 * Service to get progress / reports  information for Task Executions
 */
public interface TaskExecutionReportService {

    /**
     * @return Amount of records that have been processed
     * by the last bolt of a topology.
     * @throws AccessDeniedOrObjectDoesNotExistException thrown when given task does not exist
     */
    TaskInfo getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * @return Info messages for the specified task for given resourceNum range
     */
    List<SubTaskInfo> getDetailedTaskReport(String taskId, int from, int to);

    /**
     * Retrieve all errors that occurred for the given task
     *
     * @param task task identifier
     * @return task error info object
     * @throws AccessDeniedOrObjectDoesNotExistException thrown when given task does not exist
     */
    TaskErrorsInfo getGeneralTaskErrorReport(String task, int idsCount) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * Retrieve sample of identifiers for the given error type
     *
     * @param task      task identifier
     * @param errorType type of error
     * @return task error info objects with sample identifiers
     */
    TaskErrorsInfo getSpecificTaskErrorReport(String task, String errorType, int idsCount) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * check if a Task belong to specific topology
     * @param taskId task identifier

     * @throws AccessDeniedOrObjectDoesNotExistException thrown when given task does not exist
     */

    void checkIfTaskExists(String taskId, String topologyName) throws AccessDeniedOrObjectDoesNotExistException;


    Boolean checkIfReportExists(String taskId) throws AccessDeniedOrObjectDoesNotExistException;
}


