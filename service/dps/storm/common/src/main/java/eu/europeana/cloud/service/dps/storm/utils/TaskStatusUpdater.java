package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts/update given task in db. Two tables are modified {@link CassandraTablesAndColumnsNames#TASK_INFO_TABLE} and
 * {@link CassandraTablesAndColumnsNames#TASKS_BY_STATE_TABLE}<br/> NOTE: Operation is not in transaction! So on table can be
 * modified but second one not
 */
public class TaskStatusUpdater {


  private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusUpdater.class);

  private static TaskStatusUpdater instance;

  private final CassandraTaskInfoDAO taskInfoDAO;

  private final TasksByStateDAO tasksByStateDAO;

  private final String applicationIdentifier;

  public TaskStatusUpdater(CassandraTaskInfoDAO taskInfoDAO, TasksByStateDAO tasksByStateDAO,
      String applicationIdentifier) {
    this.taskInfoDAO = taskInfoDAO;
    this.tasksByStateDAO = tasksByStateDAO;
    this.applicationIdentifier = applicationIdentifier;
  }

  public static synchronized TaskStatusUpdater getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = new TaskStatusUpdater(
          CassandraTaskInfoDAO.getInstance(cassandra),
          TasksByStateDAO.getInstance(cassandra),
          "");
    }
    return instance;
  }

  public void insertTask(SubmitTaskParameters parameters) {
    long taskId = parameters.getTask().getTaskId();
    String topologyName = parameters.getTaskInfo().getTopologyName();
    TaskState newState = parameters.getTaskInfo().getState();
    TaskState oldState = taskInfoDAO.findById(taskId).map(TaskInfo::getState).orElse(null);

    updateTaskState(oldState, newState, topologyName, taskId, applicationIdentifier,
        parameters.getTopicName(), Calendar.getInstance().getTime());
    taskInfoDAO.insert(parameters.getTaskInfo());
  }

  public void setTaskCompletelyProcessed(long taskId, String info)
      throws NoHostAvailableException, QueryExecutionException {
    updateTasksByTaskStateTable(taskId, TaskState.PROCESSED);
    taskInfoDAO.setTaskCompletelyProcessed(taskId, info);
  }

  public void setTaskDropped(long taskId, String info)
      throws NoHostAvailableException, QueryExecutionException {
    updateTasksByTaskStateTable(taskId, TaskState.DROPPED);
    taskInfoDAO.setTaskDropped(taskId, info);
  }

  public void setUpdateProcessedFiles(long taskId, int processedRecordsCount, int ignoredRecordsCount,
      int deletedRecordsCount, int processedErrorsCount, int deletedErrorsCount)
      throws NoHostAvailableException, QueryExecutionException {
    taskInfoDAO.setUpdateProcessedFiles(taskId, processedRecordsCount, ignoredRecordsCount, deletedRecordsCount,
        processedErrorsCount, deletedErrorsCount);
  }

  public void updateState(long taskId, TaskState state)
      throws NoHostAvailableException, QueryExecutionException {
    updateState(taskId, state, state.getDefaultMessage());
  }

  public void updateState(long taskId, TaskState state, String info)
      throws NoHostAvailableException, QueryExecutionException {
    updateTasksByTaskStateTable(taskId, state);
    taskInfoDAO.updateState(taskId, state, info);
  }

  public void updateStatusExpectedSize(long taskId, TaskState state, int expectedSize)
      throws NoHostAvailableException, QueryExecutionException {
    LOGGER.info("Updating task {} expected size to: {}", taskId, expectedSize);
    updateTasksByTaskStateTable(taskId, state);
    taskInfoDAO.updateStatusExpectedSize(taskId, state, expectedSize);
  }

  private void updateTasksByTaskStateTable(long taskId, TaskState newState) {
    taskInfoDAO.findById(taskId).ifPresent(taskInfo ->
        updateTask(taskInfo.getTopologyName(), taskId, taskInfo.getState(), newState));
  }

  public void updateTask(String topologyName, long taskId, TaskState oldState, TaskState newState) {
    if (oldState == newState) {
      return;
    }

    Optional<TaskByTaskState> oldTask = tasksByStateDAO.findTask(oldState, topologyName, taskId);

    String applicationId = oldTask.map(TaskByTaskState::getApplicationId).orElse("");
    String topicName = oldTask.map(TaskByTaskState::getTopicName).orElse("");
    Date startTime = oldTask.map(TaskByTaskState::getStartTime).orElse(null);

    updateTaskState(oldState, newState, topologyName, taskId, applicationId, topicName, startTime);
  }

  private void updateTaskState(TaskState oldState, TaskState newState, String topologyName,
      long taskId, String applicationId, String topicName, Date startTime)
      throws NoHostAvailableException, QueryExecutionException {

    if (oldState != null && oldState != newState) {
      tasksByStateDAO.delete(oldState, topologyName, taskId);
    }
    tasksByStateDAO.insert(newState, topologyName, taskId, applicationId, topicName, startTime);
  }

  public void updatePostProcessedRecordsCount(long taskId, int postProcessedRecordsCount) {
    taskInfoDAO.updatePostProcessedRecordsCount(taskId, postProcessedRecordsCount);
  }

  public void updateExpectedPostProcessedRecordsNumber(long taskId, int expectedPostProcessedRecordsNumber) {
    taskInfoDAO.updateExpectedPostProcessedRecordsNumber(taskId, expectedPostProcessedRecordsNumber);
  }

  public void updateSubmitParameters(SubmitTaskParameters parameters) {
    long taskId = parameters.getTask().getTaskId();
    String topologyName = parameters.getTaskInfo().getTopologyName();
    TaskState newState = parameters.getTaskInfo().getState();
    TaskState oldState = taskInfoDAO.findById(taskId).map(TaskInfo::getState).orElse(null);
    updateTaskState(oldState, newState, topologyName, taskId, applicationIdentifier,
        parameters.getTopicName(), Calendar.getInstance().getTime());

    taskInfoDAO.updateSubmitParameters(parameters);
  }
}
