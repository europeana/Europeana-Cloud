package eu.europeana.cloud.service.dps.storm.notification.handler;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.Constants;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class NotificationTupleHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationTupleHandler.class);

    protected final ProcessedRecordsDAO processedRecordsDAO;
    protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected final CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected final CassandraTaskErrorsDAO taskErrorDAO;
    protected final CassandraTaskInfoDAO taskInfoDAO;
    protected final TasksByStateDAO tasksByStateDAO;
    protected final String topologyName;
    protected BatchExecutor batchExecutor;

    public NotificationTupleHandler(ProcessedRecordsDAO processedRecordsDAO,
                                           TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                           CassandraSubTaskInfoDAO subTaskInfoDAO,
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
        var recordId = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        //
        if (tupleShouldBeProcessed(taskId, recordId)) {
            config.getNotificationCacheEntry().incrementCounters(notificationTuple);
            Notification notification = prepareNotification(notificationTuple, config.getNotificationCacheEntry().getProcessed());
            List<BoundStatement> statementsToBeExecutedInBatch = new ArrayList<>();

            statementsToBeExecutedInBatch.addAll(prepareCommonStatementsForAllTuples(notification, config.getNotificationCacheEntry()));
            statementsToBeExecutedInBatch.addAll(prepareStatementsForTupleContainingLastRecord(notificationTuple, config));
            statementsToBeExecutedInBatch.addAll(prepareStatementsForErrors(notificationTuple, config.getNotificationCacheEntry()));
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

    private List<BoundStatement> prepareStatementsForErrors(NotificationTuple notificationTuple, NotificationCacheEntry nCache) {
        //
        if (!isError(notificationTuple)) {
            return Collections.emptyList();
        }
        List<BoundStatement> statementsToBeExecuted = new ArrayList<>();
        //
        ErrorNotification errorNotification = prepareErrorNotification(notificationTuple, nCache);
        var errorType = nCache.getErrorType(errorNotification.getErrorMessage());
        errorType.incrementCounter();
        statementsToBeExecuted.add(taskErrorDAO.insertErrorCounterStatement(notificationTuple.getTaskId(), errorType));
        //insert error
        if (!maximumNumberOfErrorsReached(errorType)) {
            statementsToBeExecuted.add(taskErrorDAO.insertErrorStatement(
                    prepareErrorNotification(notificationTuple, nCache)
            ));
        } else {
            LOGGER.warn("Will not store the error message because threshold reached for taskId={}. ", notificationTuple.getTaskId());
        }
        return statementsToBeExecuted;
    }

    private ErrorNotification prepareErrorNotification(NotificationTuple notificationTuple, NotificationCacheEntry nCache) {
        var resource = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        //
        //store notification errror
        var errorMessage = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.INFO_TEXT));
        var additionalInformation = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        if (!isErrorTuple(notificationTuple) && notificationTuple.getParameters().get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null) {
            errorMessage = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.UNIFIED_ERROR_MESSAGE));
            additionalInformation = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.EXCEPTION_ERROR_MESSAGE));
        }
        return ErrorNotification.builder()
                .taskId(notificationTuple.getTaskId())
                .errorType(nCache.getErrorType(errorMessage).getUuid())
                .errorMessage(errorMessage)
                .resource(resource)
                .additionalInformations(additionalInformation)
                .build();
    }

    private List<BoundStatement> prepareStatementsForTupleContainingLastRecord(NotificationTuple notificationTuple, NotificationHandlerConfig config){
        if (config.getTaskStateToBeSet().isPresent()) {
            return prepareStatementsForTupleContainingLastRecord(notificationTuple, config.getTaskStateToBeSet().get());
        } else {
            return Collections.emptyList();
        }
    }

    private List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple, NotificationHandlerConfig config){
        return prepareStatementsForRecordState(notificationTuple, config.getRecordStateToBeSet());
    }

    private List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple, RecordState recordState) {
        return Collections.singletonList(processedRecordsDAO.updateProcessedRecordStateStatement(notificationTuple.getTaskId(),
                String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE)),
                recordState));
    }

    private List<BoundStatement> prepareStatementsForTupleContainingLastRecord(NotificationTuple notificationTuple, TaskState newState) {
        List<BoundStatement> statementsToBeExecuted = new ArrayList<>();

        taskInfoDAO.findById(notificationTuple.getTaskId()).flatMap(
                task ->
                        tasksByStateDAO.findTask(task.getState(), topologyName, notificationTuple.getTaskId())).ifPresent(
                oldTaskState -> {
                    statementsToBeExecuted.add(tasksByStateDAO.deleteStatement(oldTaskState.getState(), topologyName, notificationTuple.getTaskId()));
                    statementsToBeExecuted.add(tasksByStateDAO.insertStatement(newState, topologyName, notificationTuple.getTaskId(), oldTaskState.getApplicationId(),
                            oldTaskState.getTopicName(), oldTaskState.getStartTime()));
                });
        statementsToBeExecuted.add(taskInfoDAO.updateStateStatement(notificationTuple.getTaskId(), newState, newState.getDefaultMessage()));

        return statementsToBeExecuted;
    }

    private String prepareAdditionalInfo(Map<String, Object> parameters) {
        var additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        var now = Instant.now().toEpochMilli();
        var processingTime = now - (Long) parameters.get(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS);
        return additionalInfo + " Processing time: " + processingTime;
    }

    private boolean maximumNumberOfErrorsReached(ErrorType errorType) {
        return errorType.getCount() > Constants.MAXIMUM_ERRORS_THRESHOLD_FOR_ONE_ERROR_TYPE;
    }

    private boolean isErrorTuple(NotificationTuple notificationTuple) {
        return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)).equalsIgnoreCase(RecordState.ERROR.toString());
    }
}
