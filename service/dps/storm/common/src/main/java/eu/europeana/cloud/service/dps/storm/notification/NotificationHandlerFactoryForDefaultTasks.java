package eu.europeana.cloud.service.dps.storm.notification;

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
                                                     CassandraTaskInfoDAO taskInfoDAO) {
        super(processedRecordsDAO,
                taskDiagnosticInfoDAO,
                taskStatusUpdater,
                subTaskInfoDAO,
                taskErrorDAO,
                taskInfoDAO);
    }

    public NotificationTupleHandler provide(NotificationTuple notificationTuple, int expectedSize, int count) {

        if (isError(notificationTuple)) {
            if (isLastOne(expectedSize, count)) {
                return new NotificationWithErrorForLastRecordInTask(this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO);
            } else {
                return new NotificationWithError(
                        this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO);
            }
        } else {
            if (isLastOne(expectedSize, count)) {
                return new DefaultNotificationForLastRecordInTask(
                        this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO);
            } else {
                return new DefaultNotification(
                        this.processedRecordsDAO,
                        this.taskDiagnosticInfoDAO,
                        this.taskStatusUpdater,
                        this.subTaskInfoDAO,
                        this.taskErrorDAO,
                        this.taskInfoDAO);
            }
        }
    }
}
