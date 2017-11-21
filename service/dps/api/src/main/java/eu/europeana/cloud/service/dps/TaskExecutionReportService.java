package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.SubTaskInfo;
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
     * @throws AccessDeniedOrObjectDoesNotExistException
     */
    TaskInfo getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * @return Info messages for the specified task between chunks
     */
    List<SubTaskInfo> getDetailedTaskReportBetweenChunks(String taskId, int from, int to);


    /**
     * Increases the amount of records that have been processed
     * by the last bolt of a topology by 1.
     */
    void incrTaskProgress(String taskId);
}


