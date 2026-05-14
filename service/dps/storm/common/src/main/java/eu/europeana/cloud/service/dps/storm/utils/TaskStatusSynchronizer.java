package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TaskStatusSynchronizer {

  private final TaskStatusUpdater taskStatusUpdater;
  private final CassandraTaskInfoDAO taskInfoDAO;

  private final TasksByStateDAO tasksByStateDAO;

  public TaskStatusSynchronizer(CassandraTaskInfoDAO taskInfoDAO, TasksByStateDAO tasksByStateDAO,
      TaskStatusUpdater taskStatusUpdater) {
    this.taskInfoDAO = taskInfoDAO;
    this.tasksByStateDAO = tasksByStateDAO;
    this.taskStatusUpdater = taskStatusUpdater;
  }

  public void synchronizeTasksByTaskStateFromBasicInfo(String topologyName, Collection<String> availableTopics) {
    List<TaskByTaskState> tasksFromTaskByTaskStateTableList = tasksByStateDAO.findTasksByStateAndTopology(
            Arrays.asList(EngineTaskState.PROCESSING_BY_REST_APPLICATION, EngineTaskState.QUEUED), topologyName);

    Map<Long, TaskByTaskState> tasksFromTaskByTaskStateTableMap = tasksFromTaskByTaskStateTableList.stream()
                                                                                                   .filter(
                                                                                                       info -> availableTopics.contains(
                                                                                                           info.getTopicName()))
                                                                                                   .collect(Collectors.toMap(
                                                                                                       TaskByTaskState::getId,
                                                                                                       Function.identity()));

    List<TaskInfo> tasksFromBasicInfoTable = findByIds(tasksFromTaskByTaskStateTableMap.keySet());
    List<TaskInfo> tasksToCorrect = tasksFromBasicInfoTable.stream().filter(this::isFinished).toList();
    for (TaskInfo task : tasksToCorrect) {
      taskStatusUpdater.updateTask(topologyName, task.getId(), tasksFromTaskByTaskStateTableMap.get(task.getId()).getState(),
              task.getEngineTaskState());
    }
  }

  private List<TaskInfo> findByIds(Collection<Long> taskIds) {
    return taskIds.stream().map(taskInfoDAO::findById).flatMap(Optional::stream).toList();
  }

  private boolean isFinished(TaskInfo info) {
      return info.getEngineTaskState() == EngineTaskState.DROPPED || info.getEngineTaskState() == EngineTaskState.PROCESSED;
  }

}
