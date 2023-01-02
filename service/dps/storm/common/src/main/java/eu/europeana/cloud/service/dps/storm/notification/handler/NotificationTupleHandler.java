package eu.europeana.cloud.service.dps.storm.notification.handler;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.common.model.dps.ErrorNotification;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.BatchExecutor;
import eu.europeana.cloud.service.dps.Constants;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;
import eu.europeana.enrichment.rest.client.report.Report;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationTupleHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationTupleHandler.class);
  public static final int MAX_STACKTRACE_LENGTH = 6000;

  protected final ProcessedRecordsDAO processedRecordsDAO;
  protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
  protected final NotificationsDAO subTaskInfoDAO;
  protected final CassandraTaskErrorsDAO taskErrorDAO;
  protected final CassandraTaskInfoDAO taskInfoDAO;
  protected final TasksByStateDAO tasksByStateDAO;
  protected final String topologyName;
  protected BatchExecutor batchExecutor;

  public NotificationTupleHandler(ProcessedRecordsDAO processedRecordsDAO,
      TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
      NotificationsDAO subTaskInfoDAO,
      CassandraTaskErrorsDAO taskErrorDAO,
      CassandraTaskInfoDAO taskInfoDAO,
      TasksByStateDAO tasksByStateDAO,
      BatchExecutor batchExecutor,
      String topologyName) {

    this.processedRecordsDAO = processedRecordsDAO;
    this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
    this.subTaskInfoDAO = subTaskInfoDAO;
    this.taskErrorDAO = taskErrorDAO;
    this.taskInfoDAO = taskInfoDAO;
    this.tasksByStateDAO = tasksByStateDAO;
    this.batchExecutor = batchExecutor;
    this.topologyName = topologyName;

  }

  public void handle(NotificationTuple notificationTuple, NotificationHandlerConfig config) {
    LOGGER.debug("Executing notification handler");
    long taskId = notificationTuple.getTaskId();
    var resource = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
    //
    if (tupleShouldBeProcessed(taskId, resource)) {
      config.getNotificationCacheEntry().incrementCounters(notificationTuple);
      Notification notification = prepareNotification(notificationTuple, config.getNotificationCacheEntry().getProcessed());
      List<BoundStatement> statementsToBeExecutedInBatch = new ArrayList<>();

      statementsToBeExecutedInBatch.addAll(prepareCommonStatementsForAllTuples(notification, config.getNotificationCacheEntry()));
      statementsToBeExecutedInBatch.addAll(prepareStatementsForErrors(notificationTuple, config.getNotificationCacheEntry()));
      statementsToBeExecutedInBatch.addAll(prepareStatementsForReports(notificationTuple, config.getNotificationCacheEntry()));
      statementsToBeExecutedInBatch.addAll(prepareStatementsForRecordState(notificationTuple, config));
      batchExecutor.executeAll(statementsToBeExecutedInBatch);
    }
    taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTime(notificationTuple.getTaskId(), Instant.now());
  }

  private boolean isFinished(ProcessedRecord theRecord) {
    return theRecord.getState() == RecordState.SUCCESS || theRecord.getState() == RecordState.ERROR;
  }

  private Notification prepareNotification(NotificationTuple notificationTuple, int resourceNum) {
    Map<String, Object> parameters = notificationTuple.getParameters();
    return Notification.builder()
                       .taskId(notificationTuple.getTaskId())
                       .resourceNum(resourceNum)
                       .topologyName(topologyName)
                       .resource(String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE)))
                       .state(String.valueOf(parameters.get(NotificationParameterKeys.STATE)))
                       .infoText(String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT)))
                       .additionalInformation(prepareAdditionalInfo(parameters))
                       .resultResource(String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE)))
                       .build();
  }

  private boolean tupleShouldBeProcessed(long taskId, String recordId) {
    Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
    return theRecord.isEmpty() || !isFinished(theRecord.get());
  }

  private List<BoundStatement> prepareCommonStatementsForAllTuples(Notification notification, NotificationCacheEntry nCache) {
    List<BoundStatement> statementsToBeExecuted = new ArrayList<>();

    statementsToBeExecuted.add(subTaskInfoDAO.insertNotificationStatement(notification));
    statementsToBeExecuted.add(taskInfoDAO.updateProcessedFilesStatement(notification.getTaskId(),
        nCache.getProcessedRecordsCount(),
        nCache.getIgnoredRecordsCount(),
        nCache.getDeletedRecordsCount(),
        nCache.getProcessedErrorsCount(),
        nCache.getDeletedErrorsCount()));
    statementsToBeExecuted.add(taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTimeStatement(
        notification.getTaskId(), Instant.now()
    ));
    return statementsToBeExecuted;
  }

  private boolean isError(NotificationTuple notificationTuple) {
    return isErrorTuple(notificationTuple)
        || (notificationTuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null);
  }

  private boolean isReportPresent(NotificationTuple notificationTuple) {
    return !notificationTuple.getReportSet().isEmpty();
  }

  private List<BoundStatement> prepareStatementsForErrors(NotificationTuple notificationTuple, NotificationCacheEntry nCache) {
    if (isError(notificationTuple)) {
      ErrorNotification errorNotification = prepareErrorNotificationFromTuple(notificationTuple, nCache);
      return getStatementsToBeExecutedFromErrorNotification(notificationTuple, nCache, errorNotification);
    }
    return Collections.emptyList();
  }

  private List<BoundStatement> prepareStatementsForReports(NotificationTuple notificationTuple, NotificationCacheEntry nCache) {
    List<BoundStatement> statementsToBeExecuted = new ArrayList<>();
    if (isReportPresent(notificationTuple)) {
      List<ErrorNotification> errorNotifications = prepareErrorNotificationsFromTupleReports(notificationTuple, nCache);
      errorNotifications.forEach(
          errorNotification -> statementsToBeExecuted.addAll(
              getStatementsToBeExecutedFromErrorNotification(notificationTuple, nCache, errorNotification))
      );
    }
    return statementsToBeExecuted;
  }

  private List<BoundStatement> getStatementsToBeExecutedFromErrorNotification(NotificationTuple notificationTuple,
      NotificationCacheEntry nCache, ErrorNotification errorNotification) {
    ErrorType errorType = nCache.getErrorType(errorNotification.getErrorMessage());
    List<BoundStatement> statementsToBeExecuted = new ArrayList<>();
    errorType.incrementCounter();
    statementsToBeExecuted.add(taskErrorDAO.insertErrorCounterStatement(notificationTuple.getTaskId(), errorType));

    if (!maximumNumberOfErrorsReached(errorType)) {
      statementsToBeExecuted.add(taskErrorDAO.insertErrorStatement(
          errorNotification
      ));
    } else {
      LOGGER.warn("Will not store the error message because threshold reached for taskId={}. ", notificationTuple.getTaskId());
    }
    return statementsToBeExecuted;
  }

  private List<ErrorNotification> prepareErrorNotificationsFromTupleReports(NotificationTuple notificationTuple,
      NotificationCacheEntry nCache) {
    ArrayList<ErrorNotification> errorNotifications = new ArrayList<>();
    notificationTuple.getReportSet().forEach(
        report ->
            errorNotifications.add(prepareErrorNotificationFromReport(notificationTuple, report, nCache))
    );
    return errorNotifications;
  }

  private ErrorNotification prepareErrorNotificationFromReport(NotificationTuple notificationTuple, Report report,
      NotificationCacheEntry nCache) {
    String resource = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
    String errorMessage = String.format("Report message:%s", report.getMessage());
    String additionalInformation = String.format(
        "ReportInformation:%nMessageType:%s%nProcessingMode:%s%nHTTPStatus:%s%nValue:%s%nStackTrace:%s",
        report.getMessageType(),
        report.getMode(),
        report.getStatus(),
        report.getValue(),
        report.getStackTrace());
    return ErrorNotification.builder()
                            .taskId(notificationTuple.getTaskId())
                            .errorType(nCache.getErrorType(errorMessage).getUuid())
                            .errorMessage(errorMessage)
                            .resource(resource)
                            .additionalInformations(prepareAdditionalInformation(additionalInformation))
                            .build();
  }


  private ErrorNotification prepareErrorNotificationFromTuple(NotificationTuple notificationTuple,
      NotificationCacheEntry nCache) {
    var resource = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
    //
    //store notification error
    var errorMessage = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.INFO_TEXT));
    var additionalInformation = String.valueOf(
        notificationTuple.getParameters().get(NotificationParameterKeys.STATE_DESCRIPTION));
    if (!isErrorTuple(notificationTuple)
        && notificationTuple.getParameters().get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null) {
      errorMessage = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.UNIFIED_ERROR_MESSAGE));
      additionalInformation = String.valueOf(
          notificationTuple.getParameters().get(NotificationParameterKeys.EXCEPTION_ERROR_MESSAGE));
    }
    return ErrorNotification.builder()
                            .taskId(notificationTuple.getTaskId())
                            .errorType(nCache.getErrorType(errorMessage).getUuid())
                            .errorMessage(errorMessage)
                            .resource(resource)
                            .additionalInformations(prepareAdditionalInformation(additionalInformation))
                            .build();
  }

  private String prepareAdditionalInformation(String additionalInformation) {
    if (additionalInformation == null) {
      return "";
    } else {
      return additionalInformation.substring(0, Math.min(MAX_STACKTRACE_LENGTH, additionalInformation.length()));
    }
  }

  private List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple,
      NotificationHandlerConfig config) {
    return prepareStatementsForRecordState(notificationTuple, config.getRecordStateToBeSet());
  }

  private List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple, RecordState recordState) {
    return Collections.singletonList(processedRecordsDAO.updateProcessedRecordStateStatement(notificationTuple.getTaskId(),
        notificationTuple.getResource(),
        recordState));
  }

  public List<BoundStatement> prepareStatementsForTupleContainingLastRecord(NotificationTuple notificationTuple,
      TaskState newState, String message) {
    List<BoundStatement> statementsToBeExecuted = new ArrayList<>();

    taskInfoDAO.findById(notificationTuple.getTaskId()).flatMap(
        task ->
            tasksByStateDAO.findTask(task.getState(), topologyName, notificationTuple.getTaskId())).ifPresent(
        oldTaskState -> {
          statementsToBeExecuted.add(tasksByStateDAO.deleteStatement(
              oldTaskState.getState(), topologyName, notificationTuple.getTaskId()
          ));
          statementsToBeExecuted.add(tasksByStateDAO.insertStatement(
              newState, topologyName, notificationTuple.getTaskId(), oldTaskState.getApplicationId(),
              oldTaskState.getTopicName(), oldTaskState.getStartTime()
          ));
        });
    statementsToBeExecuted.add(taskInfoDAO.updateStateStatement(notificationTuple.getTaskId(), newState, message));

    return statementsToBeExecuted;
  }

  private Map<String, String> prepareAdditionalInfo(Map<String, Object> parameters) {
    var processingTime = Instant.now().toEpochMilli()
        - (Long) parameters.get(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS);

    return Map.of(
        NotificationsDAO.STATE_DESCRIPTION_KEY, String.valueOf(parameters.get(NotificationParameterKeys.STATE_DESCRIPTION)),
        NotificationsDAO.PROCESSING_TIME_KEY, String.valueOf(processingTime),
        NotificationsDAO.EUROPEANA_ID_KEY, String.valueOf(parameters.get(NotificationParameterKeys.EUROPEANA_ID))
    );
  }

  private boolean maximumNumberOfErrorsReached(ErrorType errorType) {
    return errorType.getCount() > Constants.MAXIMUM_ERRORS_THRESHOLD_FOR_ONE_ERROR_TYPE;
  }

  private boolean isErrorTuple(NotificationTuple notificationTuple) {
    return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE))
                 .equalsIgnoreCase(RecordState.ERROR.toString());
  }
}
