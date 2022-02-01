package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;


public class NotificationHandlerFactoryForDefaultTasks extends NotificationHandlerFactory {

    /**
     * Factory for Notification handlers for tasks that doesn't need postprocessing
     */
    public NotificationHandlerFactoryForDefaultTasks(ProcessedRecordsDAO processedRecordsDAO,
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

    public NotificationTupleHandler provide(NotificationTuple notificationTuple, int expectedSize, int processedRecordsCount) {

        if (isError(notificationTuple)) {
            if (isLastOneTupleInTask(expectedSize, processedRecordsCount)) {
                return new NotificationWithErrorForLastRecordInTask(this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO,
                        this.batchExecutor,
                        topologyName);
            } else {
                return new NotificationWithError(
                        this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO,
                        this.batchExecutor,
                        topologyName);
            }
        } else {
            if (isLastOneTupleInTask(expectedSize, processedRecordsCount)) {
                return new DefaultNotificationForLastRecordInTask(
                        this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO,
                        this.batchExecutor,
                        topologyName);
            } else {
                return new DefaultNotification(
                        this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO,
                        this.batchExecutor,
                        topologyName);
            }
        }
    }
}
