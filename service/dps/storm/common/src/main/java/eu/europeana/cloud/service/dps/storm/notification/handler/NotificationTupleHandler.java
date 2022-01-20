package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;

public abstract class NotificationTupleHandler {

    protected final TaskStatusUpdater taskStatusUpdater;
    protected final ProcessedRecordsDAO processedRecordsDAO;
    protected final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected final CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected final CassandraTaskErrorsDAO taskErrorDAO;
    protected final CassandraTaskInfoDAO taskInfoDAO;

    protected NotificationTupleHandler(ProcessedRecordsDAO processedRecordsDAO,
                                       TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                       TaskStatusUpdater taskStatusUpdater,
                                       CassandraSubTaskInfoDAO subTaskInfoDAO,
                                       CassandraTaskErrorsDAO taskErrorDAO,
                                       CassandraTaskInfoDAO taskInfoDAO){

        this.processedRecordsDAO = processedRecordsDAO;
        this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
        this.taskStatusUpdater = taskStatusUpdater;
        this.subTaskInfoDAO = subTaskInfoDAO;
        this.taskErrorDAO = taskErrorDAO;
        this.taskInfoDAO = taskInfoDAO;

    }

    public abstract void handle(NotificationTuple notificationTuple, NotificationBolt.NotificationCache nCache) throws TaskInfoDoesNotExistException;
}
