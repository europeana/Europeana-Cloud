package eu.europeana.cloud.service.dps.storm.service;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.ErrorDetails;
import eu.europeana.cloud.common.model.dps.ErrorNotification;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.conversion.SubTaskInfoConverter;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ReportDAO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Report service powered by Cassandra.
 */
public class ReportService implements TaskExecutionReportService {

  private static final String RETRIEVING_ERROR_MESSAGE = "Specified task or error type does not exist!";
  private static final String TASK_NOT_EXISTS_ERROR_MESSAGE = "The task with the provided id doesn't exist!";
  private static final int FETCH_ONE = 1;
  private final ReportDAO reportDAO;

  /**
   * Constructor of Cassandra report service.
   *
   * @param dbService instance of CassandraConnectionProvider
   */
  public ReportService(CassandraConnectionProvider dbService) {
    reportDAO = ReportDAO.getInstance(dbService);
  }


  /**
   * Retrieve progress of task with id equal to taskId.
   *
   * @param taskId id of task
   * @return taskInfo object
   * @throws AccessDeniedOrObjectDoesNotExistException thrown in case of insufficient permission or task not existing
   */
  @Override
  public TaskInfo getTaskProgress(long taskId) throws AccessDeniedOrObjectDoesNotExistException {
    TaskInfo taskInfo = reportDAO.getTaskInfoRecord(taskId);
    return Optional.ofNullable(taskInfo)
                   .orElseThrow(() -> new AccessDeniedOrObjectDoesNotExistException(TASK_NOT_EXISTS_ERROR_MESSAGE));
  }

  /**
   * Check if Task specified by id and name of topology that it belongs to exists. If it doesn't exist then exception will be
   * thrown
   *
   * @param taskId id of task
   * @param topologyName name of topology that task belong to
   */
  @Override
  public void checkIfTaskExists(long taskId, String topologyName) throws AccessDeniedOrObjectDoesNotExistException {
    TaskInfo taskInfo = reportDAO.getTaskInfoRecord(taskId);
    if (taskInfo == null || !taskInfo.getTopologyName().equals(topologyName)) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }
  }

  /**
   * Check if there is any report regarding task with id given by taskId param
   *
   * @param taskId id of task
   * @return true or false depends on if any report exists for specified task
   */
  @Override
  public boolean checkIfReportExists(long taskId) {
    return !reportDAO.getErrorTypes(taskId).isEmpty();
  }


  /**
   * Retrieve detailed task report
   *
   * @param taskId id of task
   * @param from minimum value of notification resource number
   * @param to maximum value of notification resource number
   * @return Array of SubTaskInfo class objects
   */
  @Override
  public List<SubTaskInfo> getDetailedTaskReport(long taskId, int from, int to) {
    List<SubTaskInfo> result = new ArrayList<>();
    for (int i = NotificationsDAO.bucketNumber(to); i >= NotificationsDAO.bucketNumber(from); i--) {
      List<Notification> notifications = reportDAO.getNotifications(taskId, from, to, i);
      notifications.forEach(
          notification -> result.add(SubTaskInfoConverter.fromNotification(notification))
      );
    }
    return result;
  }


  /**
   * Retrieve sample of identifiers for the given error type
   *
   * @param taskId task identifier
   * @param errorType type of error
   * @return task error info objects with sample identifiers
   */
  @Override
  public TaskErrorsInfo getSpecificTaskErrorReport(long taskId, String errorType, int idsCount)
      throws AccessDeniedOrObjectDoesNotExistException {
    TaskErrorInfo taskErrorInfo = getTaskErrorInfo(taskId, errorType);
    taskErrorInfo.setErrorDetails(retrieveErrorDetails(taskId, errorType, idsCount));
    String message = getErrorMessage(taskId, new HashMap<>(), errorType);
    taskErrorInfo.setMessage(message);
    return new TaskErrorsInfo(taskId, List.of(taskErrorInfo));
  }

  /**
   * Retrieve all errors that occurred for the given task
   *
   * @param taskId task identifier
   * @return task error info object
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  @Override
  public TaskErrorsInfo getGeneralTaskErrorReport(long taskId, int idsCount) throws AccessDeniedOrObjectDoesNotExistException {
    List<TaskErrorInfo> errors = new ArrayList<>();
    TaskErrorsInfo result = new TaskErrorsInfo(taskId, errors);

    List<ErrorType> errorTypes = reportDAO.getErrorTypes(taskId);
    if (errorTypes.isEmpty()) {
      return result;
    }
    Map<String, String> errorMessages = new HashMap<>();
    for (ErrorType errorType : errorTypes) {
      String uuid = errorType.getUuid();
      String message = getErrorMessage(taskId, errorMessages, uuid);
      List<ErrorDetails> errorDetails = retrieveErrorDetails(taskId, uuid, idsCount);
      errors.add(new TaskErrorInfo(uuid, message, errorType.getCount(), errorDetails));
    }
    return result;
  }


  /**
   * Retrieve identifiers that occurred in the error notifications for the specified task identifier and error type. Number of
   * returned identifiers is <code>idsCount</code>. Maximum value is specified in the configuration file. When there is no data
   * for the specified task or error type <code>AccessDeniedOrObjectDoesNotExistException</code> is thrown.
   *
   * @param taskId task identifier
   * @param errorType error type
   * @param idsCount number of identifiers to retrieve
   * @return list of identifiers that occurred for the specific error while processing the given task
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  private List<ErrorDetails> retrieveErrorDetails(long taskId, String errorType, int idsCount)
      throws AccessDeniedOrObjectDoesNotExistException {
    List<ErrorDetails> errorDetails = new ArrayList<>();
    if (idsCount == 0) {
      return errorDetails;
    }

    List<ErrorNotification> errorNotifications = reportDAO.getErrorNotifications(taskId, UUID.fromString(errorType), idsCount);
    if (errorNotifications.isEmpty()) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }

    errorNotifications.forEach(errorNotification ->
        errorDetails.add(new ErrorDetails(
            errorNotification.getResource(),
            errorNotification.getAdditionalInformations()
        )));
    return errorDetails;
  }


  /**
   * Retrieve the specific error message. First it tries to retrieve it from the map that caches the messages by their error type.
   * If not present it fetches one row from the table.
   *
   * @param taskId task identifier
   * @param errorMessages map of error messages
   * @param errorType error type
   * @return error message
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  private String getErrorMessage(long taskId, Map<String, String> errorMessages, String errorType)
      throws AccessDeniedOrObjectDoesNotExistException {
    String message = errorMessages.get(errorType);
    if (message == null) {
      List<ErrorNotification> errorNotifications = reportDAO.getErrorNotifications(taskId, UUID.fromString(errorType), FETCH_ONE);
      if (errorNotifications.isEmpty()) {
        throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
      }
      message = errorNotifications.get(0).getErrorMessage();
      errorMessages.put(errorType, message);
    }
    return message;
  }


  /**
   * Create task error info object and set the correct occurrence value. Exception is thrown when there is no task with the given
   * identifier or no data for the specified error type
   *
   * @param taskId task identifier
   * @param errorType error type
   * @return object initialized with the correct occurrence number
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  private TaskErrorInfo getTaskErrorInfo(long taskId, String errorType) throws AccessDeniedOrObjectDoesNotExistException {
    ErrorType errType = reportDAO.getErrorType(taskId, UUID.fromString(errorType));
    if (errType == null) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }

    TaskErrorInfo taskErrorInfo = new TaskErrorInfo();
    taskErrorInfo.setErrorType(errorType);

    taskErrorInfo.setOccurrences(errType.getCount());

    return taskErrorInfo;
  }


}
