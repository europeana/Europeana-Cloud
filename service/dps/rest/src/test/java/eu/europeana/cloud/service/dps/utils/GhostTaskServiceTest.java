package eu.europeana.cloud.service.dps.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.config.GhostTaskServiceTestContext;
import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {GhostTaskService.class, GhostTaskServiceTestContext.class})
public class GhostTaskServiceTest {

  private static final List<TaskState> ACTIVE_TASK_STATES = Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION,
      TaskState.QUEUED);
  public static final long TASK_ID = 1L;
  private static final TaskByTaskState TOPIC_INFO_1 = createTopicInfo("oai_topology_2");
  private static final TaskByTaskState TOPIC_INFO_1_UNKNOWN_TOPIC = createTopicInfo("unknown_topic");
  private static final TaskInfo VERY_OLD_TASK = createTaskInfo(70);

  private static final TaskInfo QUITE_OLD_TASK = createTaskInfo(3);
  private static final TaskInfo RECENT_TASK = createTaskInfo(1);

  private static final TaskDiagnosticInfo TASK_PROGRESSED_ON_STORM_RECENTLY =
      TaskDiagnosticInfo.builder()
                        .lastRecordFinishedOnStormTime(Instant.now().minus(1, ChronoUnit.DAYS))
                        .build();

  private static final TaskDiagnosticInfo TASK_PROGRESSED_ON_STORM_LONG_AGO =
      TaskDiagnosticInfo.builder()
                        .lastRecordFinishedOnStormTime(Instant.now().minus(3, ChronoUnit.DAYS))
                        .build();


  @Autowired
  private GhostTaskService service;

  @Autowired
  private CassandraTaskInfoDAO taskInfoDAO;

  @Autowired
  private TasksByStateDAO tasksByStateDAO;

  @Autowired
  private TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;


  @Before
  public void setup() {
    reset(tasksByStateDAO, taskInfoDAO);
    when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
        .thenReturn(Collections.singletonList(TOPIC_INFO_1));
  }

  @Test
  public void shouldReturnEmptyListIfThereAreNotAnyActiveTasks() {
    when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
        .thenReturn(Collections.emptyList());
    assertThat(service.findGhostTasks(), empty());
  }

  @Test
  public void shouldNotReturnTaskWhichIsRecentEvenIfWasNotPerformedByStormYet() {

    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(RECENT_TASK));
    when(taskDiagnosticInfoDAO.findById(TASK_ID)).thenReturn(Optional.empty());

    assertThat(service.findGhostTasks(), empty());
  }

  @Test
  public void shouldIgnoreTaskThatDidNotReserveTopicBelongingToExistingTopology() {
    when(tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES))
        .thenReturn(Collections.singletonList(TOPIC_INFO_1_UNKNOWN_TOPIC));
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(VERY_OLD_TASK));

    List<TaskInfo> ghostTasks = service.findGhostTasks();

    assertThat(ghostTasks, empty());
  }

  @Test
  public void shouldNotReturnTaskWhichIsQuiteOldButProgressedOnStormRecently() {

    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(QUITE_OLD_TASK));
    when(taskDiagnosticInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(TASK_PROGRESSED_ON_STORM_RECENTLY));

    assertThat(service.findGhostTasks(), empty());
  }

  @Test
  public void shouldReturnTaskWhichIsQuiteOldAndWasNotPerformedByStormYet() {
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(QUITE_OLD_TASK));
    when(taskDiagnosticInfoDAO.findById(TASK_ID)).thenReturn(Optional.empty());

    List<TaskInfo> ghostTasks = service.findGhostTasks();

    assertThat(ghostTasks, contains(QUITE_OLD_TASK));
  }

  @Test
  public void shouldReturnTaskWhichIsQuiteOldButProgressedOnStormLongerTimeAgo() {
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(QUITE_OLD_TASK));
    when(taskDiagnosticInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(TASK_PROGRESSED_ON_STORM_LONG_AGO));

    List<TaskInfo> ghostTasks = service.findGhostTasks();

    assertThat(ghostTasks, contains(QUITE_OLD_TASK));
  }

  @Test
  public void shouldReturnTaskWhichIsVeryOldEvenIfProgressedOnStormRecently() {
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(VERY_OLD_TASK));
    when(taskDiagnosticInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(TASK_PROGRESSED_ON_STORM_RECENTLY));

    List<TaskInfo> ghostTasks = service.findGhostTasks();

    assertThat(ghostTasks, contains(VERY_OLD_TASK));
  }

  @Test
  public void shouldReturnTaskWhichIsVeryOldAndWasNotPerformedByStormYet() {
    when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(VERY_OLD_TASK));
    when(taskDiagnosticInfoDAO.findById(TASK_ID)).thenReturn(Optional.empty());

    List<TaskInfo> ghostTasks = service.findGhostTasks();

    assertThat(ghostTasks, contains(VERY_OLD_TASK));
  }

  private static TaskInfo createTaskInfo(int sentDaysAgo) {
    TaskInfo info = TaskInfo.builder().build();
    info.setId(GhostTaskServiceTest.TASK_ID);
    info.setSentTimestamp(Date.from(Instant.now().minus(sentDaysAgo, ChronoUnit.DAYS)));
    return info;
  }

  private static TaskByTaskState createTopicInfo(String topicName) {
    return TaskByTaskState.builder()
                          .id(GhostTaskServiceTest.TASK_ID)
                          .topicName(topicName)
                          .build();
  }

}