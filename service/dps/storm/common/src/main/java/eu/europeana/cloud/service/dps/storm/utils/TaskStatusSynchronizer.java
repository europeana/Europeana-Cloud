package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
        List<TaskInfo> tasksFromTaskByTaskStateTableList = tasksByStateDAO.listAllActiveTasksInTopology(topologyName);
        Map<Long, TaskInfo> tasksFromTaskByTaskStateTableMap = tasksFromTaskByTaskStateTableList.stream().filter(info -> availableTopics.contains(info.getTopicName()))
                .collect(Collectors.toMap(TaskInfo::getId, Function.identity()));
        List<TaskInfo> tasksFromBasicInfoTable = taskInfoDAO.findByIds(tasksFromTaskByTaskStateTableMap.keySet());
        List<TaskInfo> tasksToCorrect = tasksFromBasicInfoTable.stream().filter(this::isFinished).collect(Collectors.toList());
        for (TaskInfo task : tasksToCorrect) {
            taskStatusUpdater.updateTask(topologyName, task.getId(), tasksFromTaskByTaskStateTableMap.get(task.getId()).getState().toString(), task.getState().toString());
        }
    }

    private boolean isFinished(TaskInfo info) {
        return info.getState() == TaskState.DROPPED || info.getState() == TaskState.PROCESSED;
    }

}
