package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;

public class NotificationHandlerFactoryForPostprocessinTasks extends NotificationHandlerFactory {

    /**
     * Factory for Notification handlers for tasks that needs postprocessing
     */
    public NotificationHandlerFactoryForPostprocessinTasks(ProcessedRecordsDAO processedRecordsDAO,
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
                return new NotificationWithErrorForLastRecordInPostProcessingTask(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO);
            } else {
                return new NotificationWithError(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO);
            }
        } else {
            if (isLastOne(expectedSize, count)) {
                return new DefaultNotificationForLastRecordInPostprocessingTask(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO);
            } else {
                return new DefaultNotification(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO);
            }
        }
    }
}
