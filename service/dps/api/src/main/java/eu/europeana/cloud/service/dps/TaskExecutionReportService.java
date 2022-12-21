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
   * Retrieve progress of task with id equal to taskId.
   *
   * @param taskId task identifier
   * @return Amount of records that have been processed by the last bolt of a topology.
   * @throws AccessDeniedOrObjectDoesNotExistException thrown when given task does not exist
   */
  TaskInfo getTaskProgress(long taskId) throws AccessDeniedOrObjectDoesNotExistException;

  /**
   * Retrieve detailed task report
   *
   * @param taskId id of task
   * @param from minimum value of notification resource number
   * @param to maximum value of notification resource number
   * @return Array of SubTaskInfo class objects
   */
  List<SubTaskInfo> getDetailedTaskReport(long taskId, int from, int to);

  /**
   * Retrieve all errors that occurred for the given task
   *
   * @param taskId task identifier
   * @param idsCount number of error details to retrieve for given error type
   * @return task error info object
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  TaskErrorsInfo getGeneralTaskErrorReport(long taskId, int idsCount) throws AccessDeniedOrObjectDoesNotExistException;

  /**
   * Retrieve sample of identifiers for the given error type
   *
   * @param taskId task identifier
   * @param errorType type of error
   * @param idsCount number of error details to retrieve for given error type
   * @return task error info objects with sample identifiers
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  TaskErrorsInfo getSpecificTaskErrorReport(long taskId, String errorType, int idsCount)
      throws AccessDeniedOrObjectDoesNotExistException;

  /**
   * Check if Task specified by id and name of topology that it belongs to exists. If it doesn't exist then exception will be
   * thrown
   *
   * @param taskId id of task
   * @param topologyName name of topology that task belong to
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  void checkIfTaskExists(long taskId, String topologyName) throws AccessDeniedOrObjectDoesNotExistException;

  /**
   * Check if there is any report regarding task with id given by taskId param
   *
   * @param taskId id of task
   * @return true or false depends on if any report exists for specified task
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  boolean checkIfReportExists(long taskId) throws AccessDeniedOrObjectDoesNotExistException;
}


