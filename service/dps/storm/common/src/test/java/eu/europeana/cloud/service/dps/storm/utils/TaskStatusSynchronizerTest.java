package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskStatusSynchronizerTest {

    public static final String TOPIC_1 = "topic_1";
    private static final List<String> TOPICS = Arrays.asList(TOPIC_1, "topic_2", "topic_3");

    private static final String TOPOLOGY_NAME = "test_topology";
    private static final TaskByTaskState TASK_TOPIC_INFO_1 = createTaskTopicInfo(1L, EngineTaskState.QUEUED, TOPIC_1);
    private static final TaskByTaskState TASK_TOPIC_INFO_1_UNKNOWN_TOPIC = createTaskTopicInfo(1L, EngineTaskState.QUEUED,
            "topic_unknown");
    private static final TaskInfo INFO_1 = createTaskTopicInfo(EngineTaskState.QUEUED);
    private static final TaskInfo INFO_1_OF_UNSYNCED = createTaskTopicInfo(EngineTaskState.PROCESSED);

  @Mock
  private CassandraTaskInfoDAO taskInfoDAO;

  @Mock
  private TasksByStateDAO tasksByStateDAO;

  @Mock
  private TaskStatusUpdater taskStatusUpdater;


  @InjectMocks
  private TaskStatusSynchronizer synchronizer;

    @Test
    void synchronizeShouldNotFailIfThereIsNoTask() {
        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);
        assertTrue(true);
    }

    @Test
    void synchronizedShouldRepairInconsistentData() {
        when(tasksByStateDAO.findTasksByStateAndTopology(
                Arrays.asList(EngineTaskState.PROCESSING_BY_REST_APPLICATION, EngineTaskState.QUEUED), TOPOLOGY_NAME))
                .thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1));

        when(taskInfoDAO.findById(1L)).thenReturn(Optional.of(INFO_1_OF_UNSYNCED));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskStatusUpdater).updateTask(TOPOLOGY_NAME, 1L, EngineTaskState.QUEUED, EngineTaskState.PROCESSED);
    }

    @Test
    void synchronizedShouldNotTouchTasksWithConsistentData() {
        when(tasksByStateDAO.findTasksByStateAndTopology(
                Arrays.asList(EngineTaskState.PROCESSING_BY_REST_APPLICATION, EngineTaskState.QUEUED), TOPOLOGY_NAME))
                .thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1));
        when(taskInfoDAO.findById(1L)).thenReturn(Optional.of(INFO_1));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskStatusUpdater, never()).updateTask(any(), anyLong(), any(), any());
    }


    @Test
    void synchronizedShouldOnlyConcernTasksWithTopicReservedForTopology() {
        when(tasksByStateDAO.findTasksByStateAndTopology(
                Arrays.asList(EngineTaskState.PROCESSING_BY_REST_APPLICATION, EngineTaskState.QUEUED), TOPOLOGY_NAME))
                .thenReturn(Collections.singletonList(TASK_TOPIC_INFO_1_UNKNOWN_TOPIC));

        synchronizer.synchronizeTasksByTaskStateFromBasicInfo(TOPOLOGY_NAME, TOPICS);

        verify(taskInfoDAO, never()).findById(1L);
        verify(taskStatusUpdater, never()).updateTask(any(), anyLong(), any(), any());
    }

    private static TaskByTaskState createTaskTopicInfo(Long id, EngineTaskState state, String topic) {
    return TaskByTaskState.builder()
                          .id(id)
                          .state(state)
                          .topicName(topic)
                          .build();
  }

    private static TaskInfo createTaskTopicInfo(EngineTaskState state) {
    TaskInfo info = TaskInfo.builder().build();
    info.setId(1L);
    info.setTopologyName(TOPOLOGY_NAME);
        info.setEngineTaskState(state);
    return info;
  }

}