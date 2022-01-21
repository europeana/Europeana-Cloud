package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.utils.BucketUtils;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * Handles {@link NotificationTuple} that is generated during default records processing.
 * It doesn't contain error notification, just the notification generated by the last one bolt in the processing chain.
 * It also handles record that is not the last one record in the task (changes of the task status (for example to PROCESSED) are not needed here);
 */
public class DefaultNotification extends NotificationTupleHandler {


    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNotification.class);

    public DefaultNotification(ProcessedRecordsDAO processedRecordsDAO,
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
    public void handle(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) {
        long taskId = notificationTuple.getTaskId();
        var recordId = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
        if (theRecord.isEmpty() || !isFinished(theRecord.get())) {
            notifyTask(notificationTuple, nCache, taskId);
            processedRecordsDAO.updateProcessedRecordState(taskId, recordId, RecordState.SUCCESS);
        }
        taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTime(notificationTuple.getTaskId(), Instant.now());
    }

    protected void insertRecordDetailedInformation(int resourceNum, long taskId, String resource, String state, String infoText, String additionalInfo, String resultResource) {
        subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
    }

    private boolean isFinished(ProcessedRecord theRecord) {
        return theRecord.getState() == RecordState.SUCCESS || theRecord.getState() == RecordState.ERROR;
    }

    private void notifyTask(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache, long taskId) {
        nCache.incrementCounters(notificationTuple);

        //notification table (for single record)
        storeNotification(nCache.getProcessed(), taskId, notificationTuple.getParameters());
        saveProgressCounters(taskId, nCache);
    }

    private void saveProgressCounters(long taskId, NotificationBolt.NotificationCache nCache) {
        LOGGER.info("Updating task counter for task_id = {} and counters: {}", taskId, nCache.getCountersAsText());
        taskStatusUpdater.setUpdateProcessedFiles(taskId, nCache.getProcessedRecordsCount(),
                nCache.getIgnoredRecordsCount(), nCache.getDeletedRecordsCount(),
                nCache.getProcessedErrorsCount(), nCache.getDeletedErrorsCount());
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

}
