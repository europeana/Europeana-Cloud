package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.handler.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class NotificationHandlerFactoryForPostprocessingTasksTest {


    @Test
    public void shouldProvideHandlerForDefaultNotification() {
        NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessingTasks(
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(TaskDiagnosticInfoDAO.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(CassandraSubTaskInfoDAO.class),
                Mockito.mock(CassandraTaskErrorsDAO.class),
                Mockito.mock(CassandraTaskInfoDAO.class),
                Mockito.mock(BatchExecutor.class),
                "topologyName"
        );

        NotificationTuple tuple = new NotificationTuple(1L, Collections.emptyMap());
        NotificationTupleHandler result = factory.provide(tuple, 10, 1);

        assertTrue(result instanceof DefaultNotification);
    }

    @Test
    public void shouldProvideHandlerForDefaultNotificationAndLastRecord() {
        NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessingTasks(
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(TaskDiagnosticInfoDAO.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(CassandraSubTaskInfoDAO.class),
                Mockito.mock(CassandraTaskErrorsDAO.class),
                Mockito.mock(CassandraTaskInfoDAO.class),
                Mockito.mock(BatchExecutor.class),
                "topologyName"
        );

        NotificationTuple tuple = new NotificationTuple(1L, Collections.emptyMap());
        NotificationTupleHandler result = factory.provide(tuple, 10, 9);

        assertTrue(result instanceof DefaultNotificationForLastRecordInPostprocessingTask);
    }

    @Test
    public void shouldProvideHandlerForNotificationContainingError() {
        NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessingTasks(
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(TaskDiagnosticInfoDAO.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(CassandraSubTaskInfoDAO.class),
                Mockito.mock(CassandraTaskErrorsDAO.class),
                Mockito.mock(CassandraTaskInfoDAO.class),
                Mockito.mock(BatchExecutor.class),
                "topologyName"
        );

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.STATE, RecordState.ERROR.toString());
        NotificationTuple tuple = new NotificationTuple(1L, parameters);
        NotificationTupleHandler result = factory.provide(tuple, 10, 1);

        assertTrue(result instanceof NotificationWithError);
    }

    @Test
    public void shouldProvideHandlerForNotificationContainingErrorForLastOneRecord() {
        NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessingTasks(
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(TaskDiagnosticInfoDAO.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(CassandraSubTaskInfoDAO.class),
                Mockito.mock(CassandraTaskErrorsDAO.class),
                Mockito.mock(CassandraTaskInfoDAO.class),
                Mockito.mock(BatchExecutor.class),
                "topologyName"
        );

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.STATE, RecordState.ERROR.toString());
        NotificationTuple tuple = new NotificationTuple(1L, parameters);
        NotificationTupleHandler result = factory.provide(tuple, 12, 11);

        assertTrue(result instanceof NotificationWithErrorForLastRecordInPostProcessingTask);
    }

    @Test
    public void shouldProvideHandlerForNotificationContainingUnifiedError() {
        NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessingTasks(
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(TaskDiagnosticInfoDAO.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(CassandraSubTaskInfoDAO.class),
                Mockito.mock(CassandraTaskErrorsDAO.class),
                Mockito.mock(CassandraTaskInfoDAO.class),
                Mockito.mock(BatchExecutor.class),
                "topologyName"
        );

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, "Sample error message");
        NotificationTuple tuple = new NotificationTuple(1L, parameters);
        NotificationTupleHandler result = factory.provide(tuple, 10, 1);

        assertTrue(result instanceof NotificationWithError);
    }

    @Test
    public void shouldProvideHandlerForNotificationContainingUnifiedErrorForLastOneRecord() {
        NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessingTasks(
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(TaskDiagnosticInfoDAO.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(CassandraSubTaskInfoDAO.class),
                Mockito.mock(CassandraTaskErrorsDAO.class),
                Mockito.mock(CassandraTaskInfoDAO.class),
                Mockito.mock(BatchExecutor.class),
                "topologyName"
        );

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.STATE, RecordState.ERROR.toString());
        NotificationTuple tuple = new NotificationTuple(1L, parameters);
        NotificationTupleHandler result = factory.provide(tuple, 12, 11);

        assertTrue(result instanceof NotificationWithErrorForLastRecordInPostProcessingTask);
    }
}