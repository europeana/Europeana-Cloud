package eu.europeana.cloud.service.dps.storm.notification.handler;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.Constants;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public abstract class NotificationTupleHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationWithError.class);

    protected final ProcessedRecordsDAO processedRecordsDAO;
    protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected final CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected final CassandraTaskErrorsDAO taskErrorDAO;
    protected final CassandraTaskInfoDAO taskInfoDAO;
    protected final TasksByStateDAO tasksByStateDAO;
    protected final String topologyName;
    protected BatchExecutor batchExecutor;

    protected NotificationTupleHandler(ProcessedRecordsDAO processedRecordsDAO,
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

    public void handle(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) {
        LOGGER.debug("Executing notification handler");
        long taskId = notificationTuple.getTaskId();
        var recordId = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        //
        if (tupleShouldBeProcessed(taskId, recordId)) {
            nCache.incrementCounters(notificationTuple);

            Notification notification = prepareNotification(notificationTuple, nCache.getProcessed());
            List<BoundStatement> statementsToBeExecuted = new ArrayList<>();
            statementsToBeExecuted.addAll(prepareCommonStatementsForAllTuples(notification, nCache));
            statementsToBeExecuted.addAll(prepareStatementsForTupleContainingLastRecord(notificationTuple));
            statementsToBeExecuted.addAll(prepareStatementsForTupleContainingError(notificationTuple, nCache));
            statementsToBeExecuted.addAll(prepareStatementsForRecordState(notificationTuple));
            batchExecutor.executeAll(statementsToBeExecuted);
        }
        taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTime(notificationTuple.getTaskId(), Instant.now());
    }

    protected boolean isFinished(ProcessedRecord theRecord) {
        return theRecord.getState() == RecordState.SUCCESS || theRecord.getState() == RecordState.ERROR;
    }

    protected Notification prepareNotification(NotificationTuple notificationTuple, int resourceNum) {
        Map<String, Object> parameters = notificationTuple.getParameters();
        return Notification.builder()
                .taskId(notificationTuple.getTaskId())
                .resourceNum(resourceNum)
                .topologyName(topologyName)
                .resource(String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE)))
                .state(String.valueOf(parameters.get(NotificationParameterKeys.STATE)))
                .infoText(String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT)))
                .additionalInformations(prepareAdditionalInfo(parameters))
                .resultResource(String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE)))
                .build();
    }

    protected boolean tupleShouldBeProcessed(long taskId, String recordId) {
        Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
        return theRecord.isEmpty() || !isFinished(theRecord.get());
    }

    protected List<BoundStatement> prepareCommonStatementsForAllTuples(Notification notification, NotificationBolt.NotificationCache nCache) {
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

    protected List<BoundStatement> prepareStatementsForTupleContainingError(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) {

        List<BoundStatement> statementsToBeExecuted = new ArrayList<>();
        //
        Map<String, Object> parameters = notificationTuple.getParameters();

        var resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        //
        //store notification errror
        var errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        var additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        if (!isErrorTuple(notificationTuple) && parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null) {
            errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.UNIFIED_ERROR_MESSAGE));
            additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.EXCEPTION_ERROR_MESSAGE));
        }
        var errorType = nCache.getErrorType(errorMessage);
        //update error counter
        taskErrorDAO.updateErrorCounter(notificationTuple.getTaskId(), errorType);
        //insert error
        long errorCount = taskErrorDAO.selectErrorCountsForErrorType(notificationTuple.getTaskId(), UUID.fromString(errorType));
        if (!maximumNumberOfErrorsReached(errorCount)) {
            statementsToBeExecuted.add(taskErrorDAO.insertErrorStatement(
                    ErrorNotification.builder()
                            .taskId(notificationTuple.getTaskId())
                            .errorType(errorType)
                            .errorMessage(errorMessage)
                            .resource(resource)
                            .additionalInformations(additionalInformation)
                            .build()
            ));
        } else {
            LOGGER.warn("Will not store the error message because threshold reached for taskId={}. ", notificationTuple.getTaskId());
        }
        return statementsToBeExecuted;
    }

    protected abstract List<BoundStatement> prepareStatementsForTupleContainingLastRecord(NotificationTuple notificationTuple);

    protected abstract List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple);

    protected List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple, RecordState recordState) {
        List<BoundStatement> statementsToBeExecuted = new ArrayList<>();

        statementsToBeExecuted.add(processedRecordsDAO.updateProcessedRecordStateStatement(notificationTuple.getTaskId(),
                String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE)),
                recordState));

        return statementsToBeExecuted;
    }

    protected List<BoundStatement> prepareStatementsForTupleContainingLastRecord(NotificationTuple notificationTuple, TaskState newState, String message) {
        List<BoundStatement> statementsToBeExecuted = new ArrayList<>();

        taskInfoDAO.findById(notificationTuple.getTaskId()).flatMap(
                task ->
                        tasksByStateDAO.findTask(task.getState(), topologyName, notificationTuple.getTaskId())).ifPresent(
                oldTaskState -> {
                    statementsToBeExecuted.add(tasksByStateDAO.deleteStatement(oldTaskState.getState(), topologyName, notificationTuple.getTaskId()));
                    statementsToBeExecuted.add(tasksByStateDAO.insertStatement(newState, topologyName, notificationTuple.getTaskId(), oldTaskState.getApplicationId(),
                            oldTaskState.getTopicName(), oldTaskState.getStartTime()));
                });
        statementsToBeExecuted.add(taskInfoDAO.updateStateStatement(notificationTuple.getTaskId(), newState, message));

        return statementsToBeExecuted;
    }

    private String prepareAdditionalInfo(Map<String, Object> parameters) {
        var additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        var now = Instant.now().toEpochMilli();
        var processingTime = now - (Long) parameters.get(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS);
        return additionalInfo + " Processing time: " + processingTime;
    }

    private boolean maximumNumberOfErrorsReached(long errorCount) {
        return errorCount > Constants.MAXIMUM_ERRORS_THRESHOLD_FOR_ONE_ERROR_TYPE;
    }

    private boolean isErrorTuple(NotificationTuple notificationTuple) {
        return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)).equalsIgnoreCase(RecordState.ERROR.toString());
    }
}
