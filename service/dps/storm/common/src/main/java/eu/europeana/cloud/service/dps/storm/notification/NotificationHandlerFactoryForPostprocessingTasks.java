package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.*;

public class NotificationHandlerFactoryForPostprocessingTasks extends NotificationHandlerFactory {

    /**
     * Factory for Notification handlers for tasks that needs postprocessing
     */
    public NotificationHandlerFactoryForPostprocessingTasks(ProcessedRecordsDAO processedRecordsDAO,
                                                            TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                                            CassandraSubTaskInfoDAO subTaskInfoDAO,
                                                            CassandraTaskErrorsDAO taskErrorDAO,
                                                            CassandraTaskInfoDAO taskInfoDAO,
                                                            TasksByStateDAO tasksByStateDAO,
                                                            BatchExecutor batchExecutor,
                                                            String topologyName) {
        super(processedRecordsDAO,
                taskDiagnosticInfoDAO,
                subTaskInfoDAO,
                taskErrorDAO,
                taskInfoDAO,
                tasksByStateDAO,
                batchExecutor,
                topologyName);
    }

    public NotificationTupleHandler provide(NotificationTuple notificationTuple, int expectedSize, int processedRecordsCount) {

        if (isError(notificationTuple)) {
            if (isLastOneTupleInTask(expectedSize, processedRecordsCount)) {
                return new NotificationWithErrorForLastRecordInPostProcessingTask(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        tasksByStateDAO,
                        batchExecutor,
                        topologyName);
            } else {
                return new NotificationWithError(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        tasksByStateDAO,
                        batchExecutor,
                        topologyName);
            }
        } else {
            if (isLastOneTupleInTask(expectedSize, processedRecordsCount)) {
                return new DefaultNotificationForLastRecordInPostprocessingTask(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        tasksByStateDAO,
                        batchExecutor,
                        topologyName);
            } else {
                return new DefaultNotification(
                        processedRecordsDAO,
                        taskDiagnosticInfoDAO,
                        subTaskInfoDAO,
                        taskErrorDAO,
                        taskInfoDAO,
                        tasksByStateDAO,
                        batchExecutor,
                        topologyName);
            }
        }
    }
}
