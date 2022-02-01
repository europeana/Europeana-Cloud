package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

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
                               BatchExecutor batchExecutor,
                               String topologyName) {
        super(processedRecordsDAO,
                taskDiagnosticInfoDAO,
                taskStatusUpdater,
                subTaskInfoDAO,
                taskErrorDAO,
                taskInfoDAO,
                batchExecutor,
                topologyName);
    }

    @Override
    public void handle(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) {
        LOGGER.debug("Executing notification handler");
        long taskId = notificationTuple.getTaskId();
        var recordId = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        //
        if (tupleShouldBeProcessed(taskId, recordId)) {
            nCache.incrementCounters(notificationTuple);
            Notification notification = prepareNotification(notificationTuple, nCache.getProcessed());

            batchExecutor.executeAll(
                    subTaskInfoDAO.insertNotificationStatement(notification),
                    taskInfoDAO.updateProcessedFilesStatement(taskId,
                            nCache.getProcessedRecordsCount(),
                            nCache.getIgnoredRecordsCount(),
                            nCache.getDeletedRecordsCount(),
                            nCache.getProcessedErrorsCount(),
                            nCache.getDeletedErrorsCount()),
                    processedRecordsDAO.updateProcessedRecordStateStatement(notification.getTaskId(),
                            String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE)),
                            RecordState.SUCCESS),
                    taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTimeStatement(
                            notificationTuple.getTaskId(), Instant.now()
                    )
            );
        } else {
            taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTime(notificationTuple.getTaskId(), Instant.now());
        }
    }
}
