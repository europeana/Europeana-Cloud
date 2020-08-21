package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.model.dps.TaskStateInfo;
import eu.europeana.cloud.common.model.dps.TaskTopicInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskStatusUpdaterTest {

    public static final String TOPIC_1 = "topic_1";
    private static final List<String> TOPICS = Arrays.asList(TOPIC_1, "topic_2", "topic_3");

    private static final String TOPOLOGY_NAME = "test_topology";
    private static final TaskTopicInfo TOPIC_INFO_1 = createTopicInfo(1L, TaskState.QUEUED, TOPIC_1);
    private static final TaskTopicInfo TOPIC_INFO_1_UNKNOWN_TOPIC = createTopicInfo(1L, TaskState.QUEUED, "topic_unknown");
    private static final TaskStateInfo STATE_INFO_1 = new TaskStateInfo(1L, TOPOLOGY_NAME, TaskState.QUEUED.toString());
    private static final TaskStateInfo STATE_INFO_1_OF_UNSYNCED = new TaskStateInfo(1L, TOPOLOGY_NAME, TaskState.PROCESSED.toString());


    @Mock
    private CassandraTaskInfoDAO taskInfoDAO;

    @Mock
    private TasksByStateDAO tasksByStateDAO;

    @InjectMocks
    private TaskStatusUpdater updater;

    @Test
    public void synchronizeShouldNotFailIfThereIsNoTask() {
        updater.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);
    }

    @Test
    public void synchronizedShouldRepairInconsistentData() {
        when(tasksByStateDAO.listAllTaskInfoUseInTopic(eq(TOPOLOGY_NAME))).thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findTaskStateInfos(eq(Collections.singleton(1L)))).thenReturn(Collections.singletonList(STATE_INFO_1_OF_UNSYNCED));

        updater.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(tasksByStateDAO).updateTask(eq(TOPOLOGY_NAME), eq(1L), eq(TaskState.QUEUED.toString()), eq(TaskState.PROCESSED.toString()));
    }

    @Test
    public void synchronizedShouldNotTouchTasksWithConsistentData() {
        when(tasksByStateDAO.listAllTaskInfoUseInTopic(eq(TOPOLOGY_NAME))).thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findTaskStateInfos(eq(Collections.singleton(1L)))).thenReturn(Collections.singletonList(STATE_INFO_1));

        updater.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(tasksByStateDAO, never()).updateTask(any(), anyLong(), any(), any());
    }


    @Test
    public void synchronizedShouldOnlyConcernTasksWihTopicReservedForTopology() {
        when(tasksByStateDAO.listAllTaskInfoUseInTopic(eq(TOPOLOGY_NAME))).thenReturn(Collections.singletonList(TOPIC_INFO_1_UNKNOWN_TOPIC));

        updater.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskInfoDAO,never()).findTaskStateInfos(eq(Collections.singleton(1L)));
        verify(tasksByStateDAO, never()).updateTask(any(), anyLong(), any(), any());
    }

    private static TaskTopicInfo createTopicInfo(Long id, TaskState state, String topic) {
        TaskTopicInfo info = new TaskTopicInfo();
        info.setId(id);
        info.setState(state.toString());
        info.setTopicName(topic);
        return info;
    }

}