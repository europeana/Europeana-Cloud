package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.config.GhostTaskServiceTestContext;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static eu.europeana.cloud.service.dps.config.JndiNames.JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@TestPropertySource(properties = {JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS + "=oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;validation_topology:validation_topology_1"})
@ContextConfiguration(classes = {GhostTaskService.class, GhostTaskServiceTestContext.class})
public class GhostTaskServiceTest {

    private static final List<TaskState> ACTIVE_TASK_STATES = Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED);
    private static final TaskByTaskState TOPIC_INFO_1 = createTopicInfo(1L, "oai_topology_2");
    private static final TaskByTaskState TOPIC_INFO_1_UNKNOWN_TOPIC = createTopicInfo(1L, "unknown_topic");
    private static final TaskInfo OLD_SENT_NO_STARTED_TASK_INFO_1 = createTaskInfo(1L,11);
    private static final TaskInfo NEWLY_SENT_NO_STARTED_TASK_INFO_1 = createTaskInfo(1L,9);
    private static final TaskInfo OLD_SENT_OLD_STARTED_TASK_INFO_1 = createTaskInfo(1L,11,11);
    private static final TaskInfo OLD_SENT_NEWLY_STARTED_TASK_INFO_1 = createTaskInfo(1L,11,9);
    private static final TaskInfo NEWLY_SENT_NEWLY_STARTED_TASK_INFO_1 = createTaskInfo(1L,9,9);

    @Autowired
    private GhostTaskService service;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Before
    public void setup() {
        reset(tasksByStateDAO, taskInfoDAO);
    }

    @Test
    public void findGhostTasksReturnsEmptyListIfNotTaskActive() {
        assertThat(service.findGhostTasks(), empty());
    }

    @Test
    public void findGhostTasksReturnsTaskIfItIsOldSentAndNoStarted() {
        when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_SENT_NO_STARTED_TASK_INFO_1));

        assertThat(service.findGhostTasks(), contains(OLD_SENT_NO_STARTED_TASK_INFO_1));
    }

    @Test
    public void findGhostTasksReturnsTaskIfItIsOldSentAndOldStarted() {
        when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_SENT_OLD_STARTED_TASK_INFO_1));

        assertThat(service.findGhostTasks(), contains(OLD_SENT_OLD_STARTED_TASK_INFO_1));
    }

    @Test
    public void findGhostTasksShouldIgnoreTasksThatNewlySentAndNewStarted() {
        when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_SENT_NEWLY_STARTED_TASK_INFO_1));

        assertThat(service.findGhostTasks(), empty());
    }

    @Test
    public void findGhostTasksShouldIgnoreTasksThatOldSentButNewStarted() {
        when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(NEWLY_SENT_NEWLY_STARTED_TASK_INFO_1));

        assertThat(service.findGhostTasks(), empty());
    }

    @Test
    public void findGhostTasksShouldIgnoreTasksThatNewlySentAndNotStarted() {
        when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(NEWLY_SENT_NO_STARTED_TASK_INFO_1));

        assertThat(service.findGhostTasks(), empty());
    }

    @Test
    public void findGhostTasksShouldIgnoreTasksThatNotReserveTopicBelongingToTopology() {
        when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1_UNKNOWN_TOPIC));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_SENT_NO_STARTED_TASK_INFO_1));

        assertThat(service.findGhostTasks(), empty());
    }

    private static TaskInfo createTaskInfo(Long id, int sentDaysAgo) {
        TaskInfo info = TaskInfo.builder().build();
        info.setId(id);
        info.setSentDate(Date.from(Instant.now().minus(sentDaysAgo, ChronoUnit.DAYS)));
        return info;
    }

    private static TaskInfo createTaskInfo(Long id, int sentDaysAgo, int startedDaysAgo) {
        TaskInfo info = createTaskInfo(id, sentDaysAgo);
        info.setStartDate(Date.from(Instant.now().minus(startedDaysAgo, ChronoUnit.DAYS)));
        return info;
    }

    private static TaskByTaskState createTopicInfo(Long id, String topicName) {
        return TaskByTaskState.builder()
                .id(id)
                .topicName(topicName)
                .build();
    }

}