package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskStatusSynchronizerTest {

    public static final String TOPIC_1 = "topic_1";
    private static final List<String> TOPICS = Arrays.asList(TOPIC_1, "topic_2", "topic_3");

    private static final String TOPOLOGY_NAME = "test_topology";
    private static final TaskInfo TASK_TOPIC_INFO_1 = createTaskTopicInfo(1L, TaskState.QUEUED, TOPIC_1);
    private static final TaskInfo TASK_TOPIC_INFO_1_UNKNOWN_TOPIC = createTaskTopicInfo(1L, TaskState.QUEUED, "topic_unknown");
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
    }

    @Test
    public void synchronizedShouldRepairInconsistentData() {
        when(tasksByStateDAO.listAllActiveTasksInTopology(eq(TOPOLOGY_NAME))).thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1));
        when(taskInfoDAO.findById(eq(1L))).thenReturn(Optional.of(INFO_1_OF_UNSYNCED));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskStatusUpdater).updateTask(eq(TOPOLOGY_NAME), eq(1L), eq(TaskState.QUEUED.toString()), eq(TaskState.PROCESSED.toString()));
    }

    @Test
    public void synchronizedShouldNotTouchTasksWithConsistentData() {
        when(tasksByStateDAO.listAllActiveTasksInTopology(eq(TOPOLOGY_NAME))).thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1));
        when(taskInfoDAO.findById(eq(1L))).thenReturn(Optional.of(INFO_1));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskStatusUpdater, never()).updateTask(any(), anyLong(), any(), any());
    }


    @Test
    public void synchronizedShouldOnlyConcernTasksWithTopicReservedForTopology() {
        when(tasksByStateDAO.listAllActiveTasksInTopology(eq(TOPOLOGY_NAME))).thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1_UNKNOWN_TOPIC));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskInfoDAO, never()).findById(eq(1L));
        verify(taskStatusUpdater, never()).updateTask(any(), anyLong(), any(), any());
    }

    private static TaskInfo createTaskTopicInfo(Long id, TaskState state, String topic) {
        TaskInfo info = new TaskInfo();
        info.setId(id);
        info.setState(state);
        info.setTopicName(topic);
        return info;
    }

    private static TaskInfo createTaskTopicInfo(TaskState state) {
        TaskInfo info = new TaskInfo();
        info.setId(1L);
        info.setTopologyName(TOPOLOGY_NAME);
        info.setState(state);
        return info;
    }

}