package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.Constants;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles {@link NotificationTuple} that is generated in case of error in bolts.
 * It also handles record that is not the last one record in the task (changes of the task status (for example to PROCESSED) are not needed here);
 */
public class NotificationWithError extends NotificationTupleHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationWithError.class);

    public NotificationWithError(ProcessedRecordsDAO processedRecordsDAO,
                                 TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                 TaskStatusUpdater taskStatusUpdater,
                                 CassandraSubTaskInfoDAO subTaskInfoDAO,
                                 CassandraTaskErrorsDAO taskErrorDAO,
                                 CassandraTaskInfoDAO taskInfoDAO,
                                 String topologyName) {
        super(processedRecordsDAO,
                taskDiagnosticInfoDAO,
                taskStatusUpdater,
                subTaskInfoDAO,
                taskErrorDAO,
                taskInfoDAO,
                topologyName);
    }

    @Override
    public void handle(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) throws TaskInfoDoesNotExistException {

        long taskId = notificationTuple.getTaskId();
        var recordId = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
        if (theRecord.isEmpty() || !isFinished(theRecord.get())) {
            notifyTask(notificationTuple, nCache, taskId);
            processedRecordsDAO.updateProcessedRecordState(taskId, recordId, RecordState.ERROR);
        }
        taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTime(notificationTuple.getTaskId(), Instant.now());

    }

    protected void insertRecordDetailedInformation(int resourceNum, long taskId, String resource, String state, String infoText, String additionalInfo, String resultResource) {
        subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
    }

    private void notifyTask(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache, long taskId) {
        nCache.incrementCounters(notificationTuple);

        //notification table (for single record)
        storeNotification(nCache.getProcessed(), taskId,
                notificationTuple.getParameters());

        storeNotificationError(taskId, nCache, notificationTuple);
        saveProgressCounters(taskId, nCache);
    }

    private void saveProgressCounters(long taskId, NotificationBolt.NotificationCache nCache) {
        LOGGER.info("Updating task counter for task_id = {} and counters: {}", taskId, nCache.getCountersAsText());
        taskStatusUpdater.setUpdateProcessedFiles(taskId, nCache.getProcessedRecordsCount(),
                nCache.getIgnoredRecordsCount(), nCache.getDeletedRecordsCount(),
                nCache.getProcessedErrorsCount(), nCache.getDeletedErrorsCount());
    }

    private void storeNotificationError(long taskId, NotificationBolt.NotificationCache nCache, NotificationTuple notificationTuple) {
        Map<String, Object> parameters = notificationTuple.getParameters();
        Validate.notNull(parameters);
        var errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        var additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        if (!isErrorTuple(notificationTuple) && parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null) {
            errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.UNIFIED_ERROR_MESSAGE));
            additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.EXCEPTION_ERROR_MESSAGE));
        }
        var errorType = nCache.getErrorType(errorMessage);
        var resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        updateErrorCounter(taskId, errorType);
        insertError(taskId, errorMessage, additionalInformation, errorType, resource);
    }

    private void updateErrorCounter(long taskId, String errorType) {
        taskErrorDAO.updateErrorCounter(taskId, errorType);
    }

    private void insertError(long taskId, String errorMessage, String additionalInformation, String errorType, String resource) {
        long errorCount = taskErrorDAO.selectErrorCountsForErrorType(taskId, UUID.fromString(errorType));
        if (!maximumNumberOfErrorsReached(errorCount)) {
            taskErrorDAO.insertError(taskId, errorType, errorMessage, resource, additionalInformation);
        } else {
            LOGGER.warn("Will not store the error message because threshold reached for taskId={}. ", taskId);
        }
    }

    private boolean maximumNumberOfErrorsReached(long errorCount) {
        return errorCount > Constants.MAXIMUM_ERRORS_THRESHOLD_FOR_ONE_ERROR_TYPE;
    }

    private void storeNotification(int resourceNum, long taskId, Map<String, Object> parameters) {
        Validate.notNull(parameters);
        var resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        var state = String.valueOf(parameters.get(NotificationParameterKeys.STATE));
        var infoText = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        var additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        var resultResource = String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE));
        var now = Instant.now().toEpochMilli();
        var processingTime = now - (Long) parameters.get(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS);
        additionalInfo = additionalInfo + " Processing time: " + processingTime;
        insertRecordDetailedInformation(resourceNum, taskId, resource, state, infoText, additionalInfo, resultResource);
    }

    private boolean isErrorTuple(NotificationTuple notificationTuple) {
        return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)).equalsIgnoreCase(RecordState.ERROR.toString());
    }

    private boolean isFinished(ProcessedRecord theRecord) {
        return theRecord.getState() == RecordState.SUCCESS || theRecord.getState() == RecordState.ERROR;
    }

}
