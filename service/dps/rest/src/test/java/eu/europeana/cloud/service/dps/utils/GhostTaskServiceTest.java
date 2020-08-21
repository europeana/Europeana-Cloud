package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.model.dps.TaskTopicInfo;
import eu.europeana.cloud.service.dps.config.GhostTaskServiceTestContext;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.config.JndiNames.JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@TestPropertySource(properties = {JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS + "=oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;validation_topology:validation_topology_1"})
@ContextConfiguration(classes = {GhostTaskService.class, GhostTaskServiceTestContext.class})
public class GhostTaskServiceTest {

    public static final TaskTopicInfo TOPIC_INFO_1 = createTopicInfo(1L, "oai_topology_2");
    public static final TaskTopicInfo TOPIC_INFO_1_UNKNONW_TOPIC = createTopicInfo(1L, "unknown_topic");
    public static final TaskInfo OLD_TASK_INFO_1 = createOldTaskInfo(1L);
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
    public void findGhostTasksReturnsTaskIfItIsOldAndInProcessedByRestApplicationState() {
        when(tasksByStateDAO.findTasksInGivenState(eq(TaskState.PROCESSING_BY_REST_APPLICATION)))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_TASK_INFO_1));
        assertThat(service.findGhostTasks(), contains(OLD_TASK_INFO_1));
    }

    @Test
    public void findGhostTasksReturnsTaskIfItIsOldAndInQueuedState() {
        when(tasksByStateDAO.findTasksInGivenState(eq(TaskState.QUEUED)))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_TASK_INFO_1));
        assertThat(service.findGhostTasks(), contains(OLD_TASK_INFO_1));
    }

    @Test
    public void findGhostTasksShouldIgnoreTasksThatNotReserveTopicBelongingToTopology() {
        when(tasksByStateDAO.findTasksInGivenState(eq(TaskState.QUEUED)))
                .thenReturn(Collections.singletonList(TOPIC_INFO_1_UNKNONW_TOPIC));
        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(OLD_TASK_INFO_1));

        assertThat(service.findGhostTasks(), empty());
    }

    private static TaskInfo createOldTaskInfo(Long id) {
        TaskInfo info = new TaskInfo();
        info.setId(id);
        info.setSentDate(Date.from(Instant.now().minus(11, ChronoUnit.DAYS)));
        return info;
    }

    private static TaskTopicInfo createTopicInfo(Long id, String topicName) {
        TaskTopicInfo info = new TaskTopicInfo();
        info.setId(id);
        info.setTopicName(topicName);
        return info;
    }

}