package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;


/**
 * Service to get progress / notification information for Task Executions
 */
public interface TaskExecutionReportService {

    /**
     * @return Amount of records that have been processed 
     * by the last bolt of a topology.
     * 
     * @throws AccessDeniedOrObjectDoesNotExistException 
     */
    String getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * @return Info messages for the specified task
     */
    String getTaskNotification(String taskId);
    
    /**
     * Increases the amount of records that have been processed 
     * by the last bolt of a topology by 1.
     */
    void incrTaskProgress(String taskId);
}
