package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;

/**
 * Inserts/update given task in db. Two tables are modified {@link CassandraTablesAndColumnsNames#BASIC_INFO_TABLE}
 * and {@link CassandraTablesAndColumnsNames#TASKS_BY_STATE_TABLE}<br/>
 * NOTE: Operation is not in transaction! So on table can be modified but second one not
 *
 */
public class TaskStatusUpdater {


    public TaskStatusUpdater(CassandraTaskInfoDAO taskInfoDAO, TasksByStateDAO tasksByStateDAO, String applicationIdentifier) {
        this.taskInfoDAO = taskInfoDAO;
        this.tasksByStateDAO = tasksByStateDAO;
        this.applicationIdentifier = applicationIdentifier;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusUpdater.class);

    private static TaskStatusUpdater instance;

    private CassandraTaskInfoDAO taskInfoDAO;

    private TasksByStateDAO tasksByStateDAO;

    private String applicationIdentifier;

    public static synchronized TaskStatusUpdater getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new TaskStatusUpdater(
                    CassandraTaskInfoDAO.getInstance(cassandra),
                    new TasksByStateDAO(cassandra),
                    "");
        }
        return instance;
    }

    public void insertTask(SubmitTaskParameters parameters) {
        long taskId = parameters.getTask().getTaskId();
        String topologyName = parameters.getTopologyName();
        String state = parameters.getStatus().toString();
        tasksByStateDAO.insert(taskInfoDAO.findTaskStatus(taskId), state, topologyName, taskId, applicationIdentifier, parameters.getTopicName(), new Date());
        taskInfoDAO.insert(taskId, topologyName, parameters.getExpectedSize(), 0, state, parameters.getInfo(), parameters.getSentTime(), new Date(), null, 0, parameters.getTaskJSON());
    }

    public void updateTask(long taskId, String info, String state, Date startDate)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId, state);
        taskInfoDAO.updateTask(taskId, info, state, startDate);
    }

    public void setTaskCompletelyProcessed(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId, TaskState.PROCESSED.toString());
        taskInfoDAO.setTaskCompletelyProcessed(taskId, info);
    }

    public void setTaskDropped(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId, TaskState.DROPPED.toString());
        taskInfoDAO.setTaskDropped(taskId, info);
    }

    public void setUpdateExpectedSize(long taskId, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        taskInfoDAO.setUpdateExpectedSize(taskId, expectedSize);
    }

    public void endTask(long taskId, int processeFilesCount, int errors, String info, String state, Date finishDate)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId, state);
        taskInfoDAO.endTask(taskId, processeFilesCount, errors, info, state, finishDate);
    }

    public void setUpdateProcessedFiles(long taskId, int processedFilesCount, int errors)
            throws NoHostAvailableException, QueryExecutionException {
        taskInfoDAO.setUpdateProcessedFiles(taskId, processedFilesCount, errors);
    }

    public void updateRetryCount(long taskId, int retryCount) {
        taskInfoDAO.updateRetryCount(taskId, retryCount);
    }

    public void updateStatusExpectedSize(long taskId, String state, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        LOGGER.info("Updating task {} expected size to: {}", taskId, expectedSize);
        updateTasksByTaskStateTable(taskId, state);
        taskInfoDAO.updateStatusExpectedSize(taskId, state, expectedSize);
    }

    private void updateTasksByTaskStateTable(long taskId, String newState) {
        Optional<TaskInfo> oldTask = taskInfoDAO.findById(taskId);
        if (oldTask.isPresent()) {
            tasksByStateDAO.updateTask(oldTask.get().getTopologyName(), taskId, oldTask.get().getState().toString(), newState);
        }
    }

}
