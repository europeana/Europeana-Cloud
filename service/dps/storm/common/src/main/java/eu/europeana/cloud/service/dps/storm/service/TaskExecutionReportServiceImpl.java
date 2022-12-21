package eu.europeana.cloud.service.dps.storm.service;

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
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Report service powered by Cassandra.
 */
public class TaskExecutionReportServiceImpl implements TaskExecutionReportService {

  private static final String RETRIEVING_ERROR_MESSAGE = "Specified task or error type does not exist!";
  private static final String TASK_NOT_EXISTS_ERROR_MESSAGE = "The task with the provided id doesn't exist!";
  private static final int FETCH_ONE = 1;
  private final NotificationsDAO notificationsDAO;
  private final CassandraTaskErrorsDAO taskErrorsDAO;
  private final CassandraTaskInfoDAO taskInfoDAO;

  /**
   * Constructor of Report service
   *
   * @param notificationsDAO instance of notificationDAO
   * @param taskErrorsDAO instance of CassandraTaskErrorsDAO
   * @param taskInfoDAO instance of CassandraTaskInfoDAO
   */
  public TaskExecutionReportServiceImpl(NotificationsDAO notificationsDAO, CassandraTaskErrorsDAO taskErrorsDAO,
      CassandraTaskInfoDAO taskInfoDAO) {
    this.taskErrorsDAO = taskErrorsDAO;
    this.notificationsDAO = notificationsDAO;
    this.taskInfoDAO = taskInfoDAO;
  }


  @Override
  public TaskInfo getTaskProgress(long taskId) throws AccessDeniedOrObjectDoesNotExistException {
    Optional<TaskInfo> taskInfo = taskInfoDAO.findById(taskId);
    return taskInfo.orElseThrow(() -> new AccessDeniedOrObjectDoesNotExistException(TASK_NOT_EXISTS_ERROR_MESSAGE));
  }


  @Override
  public void checkIfTaskExists(long taskId, String topologyName) throws AccessDeniedOrObjectDoesNotExistException {
    Optional<TaskInfo> taskInfo = taskInfoDAO.findById(taskId);
    if (taskInfo.isEmpty() || !taskInfo.get().getTopologyName().equals(topologyName)) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }
  }

  @Override
  public boolean checkIfReportExists(long taskId) {
    return !taskErrorsDAO.getErrorTypes(taskId).isEmpty();
  }


  @Override
  public List<SubTaskInfo> getDetailedTaskReport(long taskId, int from, int to) {
    List<SubTaskInfo> result = new ArrayList<>();
    for (int i = NotificationsDAO.bucketNumber(to); i >= NotificationsDAO.bucketNumber(from); i--) {
      List<Notification> notifications = notificationsDAO.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(taskId,
          from, to, i);
      notifications.forEach(
          notification -> result.add(SubTaskInfoConverter.fromNotification(notification))
      );
    }
    return result;
  }


  @Override
  public TaskErrorsInfo getSpecificTaskErrorReport(long taskId, String errorType, int idsCount)
      throws AccessDeniedOrObjectDoesNotExistException {
    TaskErrorInfo taskErrorInfo = getTaskErrorInfo(taskId, errorType);
    taskErrorInfo.setErrorDetails(retrieveErrorDetails(taskId, errorType, idsCount));
    String message = getErrorMessage(taskId, new HashMap<>(), errorType);
    taskErrorInfo.setMessage(message);
    return new TaskErrorsInfo(taskId, List.of(taskErrorInfo));
  }


  @Override
  public TaskErrorsInfo getGeneralTaskErrorReport(long taskId, int idsCount) throws AccessDeniedOrObjectDoesNotExistException {
    List<TaskErrorInfo> errors = new ArrayList<>();
    TaskErrorsInfo result = new TaskErrorsInfo(taskId, errors);

    List<ErrorType> errorTypes = taskErrorsDAO.getErrorTypes(taskId);
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

    List<ErrorNotification> errorNotifications = taskErrorsDAO.getErrorNotificationsWithGivenLimit(taskId,
        UUID.fromString(errorType), idsCount);
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
      List<ErrorNotification> errorNotifications = taskErrorsDAO.getErrorNotificationsWithGivenLimit(taskId,
          UUID.fromString(errorType), FETCH_ONE);
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
    return taskErrorsDAO.getErrorType(taskId, UUID.fromString(errorType))
                        .map(eT ->
                            TaskErrorInfo.builder()
                                         .errorType(errorType)
                                         .occurrences(eT.getCount())
                                         .build())
                        .orElseThrow(() -> new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE));
  }
}
