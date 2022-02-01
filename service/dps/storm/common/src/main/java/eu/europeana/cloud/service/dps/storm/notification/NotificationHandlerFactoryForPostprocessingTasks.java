package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;

public class NotificationHandlerFactoryForPostprocessingTasks extends NotificationHandlerFactory {

    /**
     * Factory for Notification handlers for tasks that needs postprocessing
     */
    public NotificationHandlerFactoryForPostprocessingTasks(ProcessedRecordsDAO processedRecordsDAO,
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
                return new NotificationWithErrorForLastRecordInPostProcessingTask(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        batchExecutor,
                        topologyName);
            } else {
                return new NotificationWithError(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        batchExecutor,
                        topologyName);
            }
        } else {
            if (isLastOneTupleInTask(expectedSize, processedRecordsCount)) {
                return new DefaultNotificationForLastRecordInPostprocessingTask(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        batchExecutor,
                        topologyName);
            } else {
                return new DefaultNotification(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        taskStatusUpdater,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        batchExecutor,
                        topologyName);
            }
        }
    }
}
