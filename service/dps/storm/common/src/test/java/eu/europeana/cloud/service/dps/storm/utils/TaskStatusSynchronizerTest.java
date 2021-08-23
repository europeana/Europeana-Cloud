package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TaskStatusSynchronizerTest {

    public static final String TOPIC_1 = "topic_1";
    private static final List<String> TOPICS = Arrays.asList(TOPIC_1, "topic_2", "topic_3");

    private static final String TOPOLOGY_NAME = "test_topology";
    private static final TaskByTaskState TASK_TOPIC_INFO_1 = createTaskTopicInfo(1L, TaskState.QUEUED, TOPIC_1);
    private static final TaskByTaskState TASK_TOPIC_INFO_1_UNKNOWN_TOPIC = createTaskTopicInfo(1L, TaskState.QUEUED, "topic_unknown");
    private static final TaskInfo INFO_1 = createTaskTopicInfo(TaskState.QUEUED);
    private static final TaskInfo INFO_1_OF_UNSYNCED = createTaskTopicInfo(TaskState.PROCESSED);

    @Mock
    private CassandraTaskInfoDAO taskInfoDAO;

    @Mock
    private TasksByStateDAO tasksByStateDAO;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;


    @InjectMocks
    private TaskStatusSynchronizer synchronizer;

    @Test
    public void synchronizeShouldNotFailIfThereIsNoTask() {
        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);
        Assert.assertTrue(true);
    }

    @Test
    public void synchronizedShouldRepairInconsistentData() {
        when(tasksByStateDAO.findTasksByStateAndTopology(
                Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED),TOPOLOGY_NAME))
                .thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1));

        when(taskInfoDAO.findById(1L)).thenReturn(Optional.of(INFO_1_OF_UNSYNCED));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskStatusUpdater).updateTask(TOPOLOGY_NAME, 1L, TaskState.QUEUED, TaskState.PROCESSED);
    }

    @Test
    public void synchronizedShouldNotTouchTasksWithConsistentData() {
        when(tasksByStateDAO.findTasksByStateAndTopology(
                Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED), TOPOLOGY_NAME))
                .thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1));
        when(taskInfoDAO.findById(1L)).thenReturn(Optional.of(INFO_1));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskStatusUpdater, never()).updateTask(any(), anyLong(), any(), any());
    }


    @Test
    public void synchronizedShouldOnlyConcernTasksWithTopicReservedForTopology() {
        when(tasksByStateDAO.findTasksByStateAndTopology(
                Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED), TOPOLOGY_NAME))
                .thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1_UNKNOWN_TOPIC));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskInfoDAO, never()).findById(1L);
        verify(taskStatusUpdater, never()).updateTask(any(), anyLong(), any(), any());
    }

    private static TaskByTaskState createTaskTopicInfo(Long id, TaskState state, String topic) {
        return TaskByTaskState.builder()
                .id(id)
                .state(state)
                .topicName(topic)
                .build();
    }

    private static TaskInfo createTaskTopicInfo(TaskState state) {
        TaskInfo info = TaskInfo.builder().build();
        info.setId(1L);
        info.setTopologyName(TOPOLOGY_NAME);
        info.setState(state);
        return info;
    }

}