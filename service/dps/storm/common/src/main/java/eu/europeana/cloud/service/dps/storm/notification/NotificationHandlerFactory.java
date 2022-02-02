package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationTupleHandler;

/**
 * Abstract factory for notification tuple handlers;
 */
public abstract class NotificationHandlerFactory {

    protected final ProcessedRecordsDAO processedRecordsDAO;
    protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected final CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected final CassandraTaskErrorsDAO taskErrorDAO;
    protected final CassandraTaskInfoDAO taskInfoDAO;
    protected  final TasksByStateDAO tasksByStateDAO;
    protected BatchExecutor batchExecutor;
    protected final String topologyName;

    protected NotificationHandlerFactory(ProcessedRecordsDAO processedRecordsDAO,
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

    public abstract NotificationTupleHandler provide(NotificationTuple notificationTuple, int expectedSize, int processedRecordsCount);

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
