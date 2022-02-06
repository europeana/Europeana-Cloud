package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.*;

/**
 * Abstract factory for notification tuple handlers;
 */
public class NotificationHandlerFactory {

    protected final ProcessedRecordsDAO processedRecordsDAO;
    protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected final CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected final CassandraTaskErrorsDAO taskErrorDAO;
    protected final CassandraTaskInfoDAO taskInfoDAO;
    protected  final TasksByStateDAO tasksByStateDAO;
    protected BatchExecutor batchExecutor;
    protected final String topologyName;

    public NotificationHandlerFactory(ProcessedRecordsDAO processedRecordsDAO,
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

    public NotificationTupleHandler provide(NotificationTuple notificationTuple, int expectedSize, int processedRecordsCount, boolean needsPostprocessing){
        if (isLastOneTupleInTask(expectedSize, processedRecordsCount)) {
            return notificationTupleHandlerForLastRecord(notificationTuple, needsPostprocessing);
        } else {
            return notificationTupleHandlerForMiddleRecord(notificationTuple);
        }
    }

    private NotificationTupleHandler notificationTupleHandlerForLastRecord(NotificationTuple notificationTuple, boolean needsPostprocessing) {
        if (isError(notificationTuple)) {
            return new NotificationWithErrorForLastRecordInTask(
                    processedRecordsDAO,
                    taskDiagnosticInfoDAO,
                    subTaskInfoDAO,
                    taskErrorDAO,
                    taskInfoDAO,
                    tasksByStateDAO,
                    batchExecutor,
                    topologyName,
                    needsPostprocessing ? TaskState.READY_FOR_POST_PROCESSING : TaskState.PROCESSED
            );
        } else {
            return new DefaultNotificationForLastRecordInTask(
                    processedRecordsDAO,
                    taskDiagnosticInfoDAO,
                    subTaskInfoDAO,
                    taskErrorDAO,
                    taskInfoDAO,
                    tasksByStateDAO,
                    batchExecutor,
                    topologyName,
                    needsPostprocessing ? TaskState.READY_FOR_POST_PROCESSING : TaskState.PROCESSED
            );
        }
    }

    private NotificationTupleHandler notificationTupleHandlerForMiddleRecord(NotificationTuple notificationTuple) {
        if (isError(notificationTuple)) {
            return new NotificationWithError(
                    processedRecordsDAO,
                    taskDiagnosticInfoDAO,
                    subTaskInfoDAO,
                    taskErrorDAO,
                    taskInfoDAO,
                    tasksByStateDAO,
                    batchExecutor,
                    topologyName
            );
        } else {
            return new DefaultNotification(
                    processedRecordsDAO,
                    taskDiagnosticInfoDAO,
                    subTaskInfoDAO,
                    taskErrorDAO,
                    taskInfoDAO,
                    tasksByStateDAO,
                    batchExecutor,
                    topologyName
            );
        }
    }

    protected boolean isError(NotificationTuple notificationTuple) {
        return isErrorTuple(notificationTuple)
                || (notificationTuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null);
    }

    protected boolean isErrorTuple(NotificationTuple notificationTuple) {
        return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)).equalsIgnoreCase(RecordState.ERROR.toString());
    }

    protected boolean isLastOneTupleInTask(int expectedSize, int processedRecordsCount) {
        return processedRecordsCount + 1 == expectedSize;
    }
}
