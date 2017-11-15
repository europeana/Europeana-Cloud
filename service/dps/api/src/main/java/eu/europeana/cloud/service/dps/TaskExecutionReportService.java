package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;


/**
 * Service to get progress / reports  information for Task Executions
 */
public interface TaskExecutionReportService {

    /**
     * @return Amount of records that have been processed
     * by the last bolt of a topology.
     * @throws AccessDeniedOrObjectDoesNotExistException
     */
    String getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * @return Info messages for the specified task between chunks
     */
    String getDetailedTaskReportBetweenChunks(String taskId, int from, int to);


    /**
     * Increases the amount of records that have been processed
     * by the last bolt of a topology by 1.
     */
    void incrTaskProgress(String taskId);
}


