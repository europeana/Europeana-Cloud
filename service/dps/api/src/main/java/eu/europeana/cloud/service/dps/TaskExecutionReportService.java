package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.StatisticsReport;
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

    /**
     * Returns statistics report for a given task. The report is only available if validation was run and there were correctly validated records.
     *
     * @param taskId task identifier
     * @return statistics report
     */
    StatisticsReport getTaskStatisticsReport(String taskId);


    /**
     * Retrieve all errors that occurred for the given task
     *
     * @param task task identifier
     * @return task error info object
     * @throws AccessDeniedOrObjectDoesNotExistException
     */
    TaskErrorsInfo getGeneralTaskErrorReport(String task) throws AccessDeniedOrObjectDoesNotExistException;

    /**
     * Retrieve sample of identifiers for the given error type
     *
     * @param task task identifier
     * @param errorType type of error
     *
     * @return task error info objects with sample identifiers
     */
    TaskErrorsInfo getSpecificTaskErrorReport(String task, String errorType) throws AccessDeniedOrObjectDoesNotExistException;
}


