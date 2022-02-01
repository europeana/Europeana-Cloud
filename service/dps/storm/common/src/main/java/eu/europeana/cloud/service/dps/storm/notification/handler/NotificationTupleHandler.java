package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public abstract class NotificationTupleHandler {

    protected final TaskStatusUpdater taskStatusUpdater;
    protected final ProcessedRecordsDAO processedRecordsDAO;
    protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected final CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected final CassandraTaskErrorsDAO taskErrorDAO;
    protected final CassandraTaskInfoDAO taskInfoDAO;
    protected final String topologyName;
    protected BatchExecutor batchExecutor;

    protected NotificationTupleHandler(ProcessedRecordsDAO processedRecordsDAO,
                                       TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                       TaskStatusUpdater taskStatusUpdater,
                                       CassandraSubTaskInfoDAO subTaskInfoDAO,
                                       CassandraTaskErrorsDAO taskErrorDAO,
                                       CassandraTaskInfoDAO taskInfoDAO,
                                       BatchExecutor batchExecutor,
                                       String topologyName) {

        this.processedRecordsDAO = processedRecordsDAO;
        this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
        this.taskStatusUpdater = taskStatusUpdater;
        this.subTaskInfoDAO = subTaskInfoDAO;
        this.taskErrorDAO = taskErrorDAO;
        this.taskInfoDAO = taskInfoDAO;
        this.batchExecutor = batchExecutor;
        this.topologyName = topologyName;

    }

    public abstract void handle(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) throws TaskInfoDoesNotExistException;

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

    private String prepareAdditionalInfo(Map<String, Object> parameters) {
        var additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        var now = Instant.now().toEpochMilli();
        var processingTime = now - (Long) parameters.get(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS);
        return additionalInfo + " Processing time: " + processingTime;
    }
}
