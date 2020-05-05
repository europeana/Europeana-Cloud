package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.model.dps.TaskStateInfo;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;

@Service
public class TaskStatusUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusUpdater.class);

    private static TaskStatusUpdater instance;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private String applicationIdentifier;

    public static synchronized TaskStatusUpdater getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new TaskStatusUpdater();
            instance.taskInfoDAO=CassandraTaskInfoDAO.getInstance(cassandra);
            instance.tasksByStateDAO=new TasksByStateDAO(cassandra);
            instance.applicationIdentifier="";
        }
        return instance;
    }

    public TaskInfo searchById(long taskId)
            throws NoHostAvailableException, QueryExecutionException, TaskInfoDoesNotExistException {
       return taskInfoDAO.searchById(taskId);
    }

    /**
     * Inserts/update given task in db. Two tables are modified {@link CassandraTablesAndColumnsNames#BASIC_INFO_TABLE}
     * and {@link CassandraTablesAndColumnsNames#TASKS_BY_STATE_TABLE}<br/>
     * NOTE: Operation is not in transaction! So on table can be modified but second one not
     * Parameters corresponding to names of column in table(s)
     *
     * @param expectedSize
     * @param state
     * @param info
     */

    public void insertTask(long taskId, String topologyName, int expectedSize, String state, String info, String topicName) {
        insert(taskId, topologyName, expectedSize, state, info, new Date(), "", "");
    }

    public void insert(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime, String taskInformations)
            throws NoHostAvailableException, QueryExecutionException {
        insert(taskId, topologyName, expectedSize, state, info, sentTime, taskInformations, "");
    }

    public void insert(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime, String taskInformations, String topicName)
            throws NoHostAvailableException, QueryExecutionException {
        tasksByStateDAO.insert(taskInfoDAO.findTaskStatus(taskId), state, topologyName, taskId, applicationIdentifier, topicName);
        taskInfoDAO.insert(taskId, topologyName, expectedSize, 0, state, info, sentTime, null, null, 0, taskInformations);

    }

    public void insert(long taskId, String topologyName, int expectedSize, int processedFilesCount, String state, String info, Date sentTime, Date startTime, Date finishTime, int errors, String taskInformations)
            throws NoHostAvailableException, QueryExecutionException {
        tasksByStateDAO.insert(taskInfoDAO.findTaskStatus(taskId), state, topologyName, taskId, applicationIdentifier, "");
        taskInfoDAO.insert(taskId, topologyName, expectedSize, processedFilesCount, state, info, sentTime, startTime, finishTime, errors, taskInformations);
    }

    public void insert(long taskId, String topologyName, int expectedSize, String state, String info, String topicName) throws NoHostAvailableException, QueryExecutionException {
        tasksByStateDAO.insert(taskInfoDAO.findTaskStatus(taskId), state, topologyName, taskId, applicationIdentifier, topicName);
        taskInfoDAO.insert(taskId, topologyName, expectedSize, state, info, applicationIdentifier, topicName);
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

    public void updateStatusExpectedSize(long taskId, String state, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        LOGGER.info("Updating task {} expected size to: {}", taskId, expectedSize);
        updateTasksByTaskStateTable(taskId, state);
        taskInfoDAO.updateStatusExpectedSize(taskId, state, expectedSize);
    }

    private void updateTasksByTaskStateTable(long taskId, String newState) {
        Optional<TaskStateInfo> oldTask = taskInfoDAO.findTaskStateInfo(taskId);
        if (oldTask.isPresent()) {
            tasksByStateDAO.updateTask(oldTask.get().getTopologyName(), taskId, oldTask.get().getState(), newState);
        }
    }

}
