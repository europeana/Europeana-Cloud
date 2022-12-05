package eu.europeana.cloud.service.dps.storm.service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.ErrorDetails;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.conversion.TaskInfoConverter;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ReportDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    reportDAO = new ReportDAO(dbService);
  }


  /**
   * Retrieve progress of task with id equal to taskId.
   *
   * @param taskId id of task
   * @return taskInfo object
   * @throws AccessDeniedOrObjectDoesNotExistException thrown in case of insufficient permission or task not existing
   */
  @Override
  public TaskInfo getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException {
    long taskIdValue = Long.parseLong(taskId);
    Row taskInfo = reportDAO.getTaskInfoRecord(taskIdValue);
    if (taskInfo != null) {
      return TaskInfoConverter.fromDBRow(taskInfo);
    }
    throw new AccessDeniedOrObjectDoesNotExistException(TASK_NOT_EXISTS_ERROR_MESSAGE);
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
  public List<SubTaskInfo> getDetailedTaskReport(String taskId, int from, int to) {
    long taskIdValue = Long.parseLong(taskId);
    List<SubTaskInfo> result = new ArrayList<>();
    for (int i = NotificationsDAO.bucketNumber(to); i >= NotificationsDAO.bucketNumber(from); i--) {
      ResultSet detailedTaskReportResultSet = reportDAO.getNotification(taskIdValue, from, to, i);
      result.addAll(convertDetailedTaskReportToListOfSubTaskInfo(detailedTaskReportResultSet));
    }

    return result;
  }


  private List<SubTaskInfo> convertDetailedTaskReportToListOfSubTaskInfo(ResultSet data) {
    List<SubTaskInfo> subTaskInfoList = new ArrayList<>();

    for (Row row : data) {
      Map<String, String> additionalInformationMap =
          row.getMap(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATION, String.class, String.class);

      SubTaskInfo subTaskInfo = new SubTaskInfo(row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM),
          row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE),
          RecordState.valueOf(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE)),
          row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT),
          additionalInformationMap.get(NotificationsDAO.STATE_DESCRIPTION_KEY),
          additionalInformationMap.get(NotificationsDAO.EUROPEANA_ID_KEY),
          additionalInformationMap.get(NotificationsDAO.PROCESSING_TIME_KEY) !=
              null ? Long.parseLong(additionalInformationMap.get(NotificationsDAO.PROCESSING_TIME_KEY)) : 0L,
          row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE));
      subTaskInfoList.add(subTaskInfo);
    }
    return subTaskInfoList;
  }

  /**
   * Retrieve all errors that occurred for the given task
   *
   * @param task task identifier
   * @return task error info object
   * @throws AccessDeniedOrObjectDoesNotExistException in case of missing task definition
   */
  @Override
  public TaskErrorsInfo getGeneralTaskErrorReport(String task, int idsCount) throws AccessDeniedOrObjectDoesNotExistException {
    long taskId = Long.parseLong(task);
    List<TaskErrorInfo> errors = new ArrayList<>();
    TaskErrorsInfo result = new TaskErrorsInfo(taskId, errors);

    ResultSet rs = reportDAO.getErrorStatements(taskId);
    if (!rs.iterator().hasNext()) {
      return result;
    }

    Map<String, String> errorMessages = new HashMap<>();

    while (rs.iterator().hasNext()) {
      Row row = rs.one();

      String errorType = row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE).toString();
      String message = getErrorMessage(taskId, errorMessages, errorType);
      int occurrences = row.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER);
      List<ErrorDetails> errorDetails = retrieveErrorDetails(taskId, errorType, idsCount);
      errors.add(new TaskErrorInfo(errorType, message, occurrences, errorDetails));
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

    ResultSet rs = reportDAO.getErrorStatement(taskId, UUID.fromString(errorType), idsCount);
    if (!rs.iterator().hasNext()) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }

    while (rs.iterator().hasNext()) {
      Row row = rs.one();
      errorDetails.add(new ErrorDetails(
          row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE),
          row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ADDITIONAL_INFORMATIONS)
      ));
    }
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
      ResultSet rs = reportDAO.getErrorStatement(taskId, UUID.fromString(errorType), FETCH_ONE);
      if (!rs.iterator().hasNext()) {
        throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
      }
      message = rs.one().getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE);
      errorMessages.put(errorType, message);
    }
    return message;
  }


  /**
   * Retrieve sample of identifiers for the given error type
   *
   * @param task task identifier
   * @param errorType type of error
   * @return task error info objects with sample identifiers
   */
  @Override
  public TaskErrorsInfo getSpecificTaskErrorReport(String task, String errorType, int idsCount)
      throws AccessDeniedOrObjectDoesNotExistException {
    long taskId = Long.parseLong(task);
    TaskErrorInfo taskErrorInfo = getTaskErrorInfo(taskId, errorType);
    taskErrorInfo.setErrorDetails(retrieveErrorDetails(taskId, errorType, idsCount));
    String message = getErrorMessage(taskId, new HashMap<>(), errorType);
    taskErrorInfo.setMessage(message);
    return new TaskErrorsInfo(taskId, List.of(taskErrorInfo));
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
    ResultSet rs = reportDAO.getErrorCounter(taskId, UUID.fromString(errorType));
    if (!rs.iterator().hasNext()) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }

    TaskErrorInfo taskErrorInfo = new TaskErrorInfo();
    taskErrorInfo.setErrorType(errorType);

    Row row = rs.one();
    taskErrorInfo.setOccurrences(row.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER));

    return taskErrorInfo;
  }


  @Override
  public void checkIfTaskExists(String taskId, String topologyName) throws AccessDeniedOrObjectDoesNotExistException {
    long taskIdValue = Long.parseLong(taskId);
    Row taskInfo = reportDAO.getTaskInfoRecord(taskIdValue);
    if (taskInfo == null || !taskInfo.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME).equals(topologyName)) {
      throw new AccessDeniedOrObjectDoesNotExistException(RETRIEVING_ERROR_MESSAGE);
    }
  }

  @Override
  public boolean checkIfReportExists(String taskId) {
    long taskIdValue = Long.parseLong(taskId);
    ResultSet rs = reportDAO.getErrorStatements(taskIdValue);
    return rs.iterator().hasNext();
  }

}
