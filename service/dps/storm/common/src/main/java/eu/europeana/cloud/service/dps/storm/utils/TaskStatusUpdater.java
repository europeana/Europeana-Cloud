package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.model.dps.TaskStateInfo;
import eu.europeana.cloud.common.model.dps.TaskTopicInfo;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public TaskInfo searchById(long taskId)
            throws NoHostAvailableException, QueryExecutionException, TaskInfoDoesNotExistException {
       return taskInfoDAO.searchById(taskId);
    }

    public void insertTask(SubmitTaskParameters parameters) {
        long taskId = parameters.getTask().getTaskId();
        String topologyName = parameters.getTopologyName();
        String state = parameters.getStatus().toString();
        tasksByStateDAO.insert(taskInfoDAO.findTaskStatus(taskId), state, topologyName, taskId, applicationIdentifier, parameters.getTopicName(),null);
        taskInfoDAO.insert(taskId, topologyName, parameters.getExpectedSize(), 0, state, parameters.getInfo(), parameters.getSentTime(), null, null, 0, parameters.getTaskJSON());
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

    public void synchronizeTasksByTaskStateFromBasicInfo(String topologyName, Collection<String> availableTopics) {
        List<TaskTopicInfo> infoList = tasksByStateDAO.listAllTaskInfoUseInTopic(topologyName);
        Map<Long, TaskTopicInfo> infoMap = infoList.stream().filter(info->availableTopics.contains(info.getTopicName()))
                .collect(Collectors.toMap(TaskTopicInfo::getId, Function.identity()));
        List<TaskStateInfo> statesFromBasicInfoTable = taskInfoDAO.findTaskStateInfos(infoMap.keySet());
        List<TaskStateInfo> tasksToCorrect = statesFromBasicInfoTable.stream().filter(this::isFinished).collect(Collectors.toList());
        for(TaskStateInfo task:tasksToCorrect){
            tasksByStateDAO.updateTask(topologyName,task.getId(), infoMap.get(task.getId()).getState(),task.getState());
        }
    }

    private boolean isFinished(TaskStateInfo info) {
        return TaskState.DROPPED.toString().equals(info.getState()) || TaskState.PROCESSED.toString().equals(info.getState());
    }

    public void updateTaskStartDate(TaskInfo taskInfo) {
        taskInfoDAO.updateTaskStartDate(taskInfo);
    }
}
