package eu.europeana.cloud.service.dps.utils;

import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.config.UnfinishedTasksContext;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@ContextConfiguration(classes = {UnfinishedTasksContext.class, UnfinishedTasksExecutor.class})
@RunWith(SpringRunner.class)
public class UnfinishedTasksExecutorTest {

  @Autowired
  private TasksByStateDAO tasksByStateDAO;

  @Autowired
  private CassandraTaskInfoDAO cassandraTaskInfoDAO;

  @Autowired
  private UnfinishedTasksExecutor unfinishedTasksExecutor;

  @Autowired
  private TaskSubmitterFactory taskSubmitterFactory;

  @Test
  public void shouldNotStartExecutionForEmptyTasksList() {
    //given
    List<TaskInfo> unfinishedTasks = new ArrayList<>();

    Mockito.reset(tasksByStateDAO);
    when(tasksByStateDAO.findTasksByState(Mockito.any(List.class))).thenReturn(unfinishedTasks);
    //when
    unfinishedTasksExecutor.restartUnfinishedTasks();
    //then
    Mockito.verify(tasksByStateDAO, Mockito.times(1)).findTasksByState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES);
  }

  @Test
  public void shouldStartExecutionForOneTasks() throws TaskInfoDoesNotExistException {
    //given
    List<TaskByTaskState> unfinishedTasks = new ArrayList<>();
    TaskByTaskState taskByTaskState = prepareTestTaskByTaskState();
    TaskInfo taskInfo = prepareTestTask();
    unfinishedTasks.add(taskByTaskState);

    Mockito.reset(tasksByStateDAO);
    Mockito.reset(taskSubmitterFactory);
    when(tasksByStateDAO.findTasksByState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES)).thenReturn(unfinishedTasks);
    when(cassandraTaskInfoDAO.findById(1L)).thenReturn(Optional.of(taskInfo));
    when(taskSubmitterFactory.provideTaskSubmitter(Mockito.any(SubmitTaskParameters.class)))
        .thenReturn(Mockito.mock(TaskSubmitter.class));
    //when
    unfinishedTasksExecutor.restartUnfinishedTasks();
    //then
    Mockito.verify(tasksByStateDAO, Mockito.times(1)).findTasksByState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES);
  }


  @Test
  public void shouldStartExecutionForTasksThatBelongsToGivenMachine() {
    //given
    List<TaskByTaskState> unfinishedTasks = new ArrayList<>();
    TaskByTaskState taskByTaskState = prepareTestTaskByTaskState();
    TaskInfo taskInfo = prepareTestTask();
    unfinishedTasks.add(taskByTaskState);
    unfinishedTasks.add(prepareTestTaskByTaskStateForAnotherMachine());

    Mockito.reset(tasksByStateDAO);
    Mockito.reset(taskSubmitterFactory);
    when(tasksByStateDAO.findTasksByState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES)).thenReturn(unfinishedTasks);
    when(cassandraTaskInfoDAO.findById(1L)).thenReturn(Optional.of(taskInfo));
    when(taskSubmitterFactory.provideTaskSubmitter(Mockito.any(SubmitTaskParameters.class))).thenReturn(
        Mockito.mock(TaskSubmitter.class));
    //when
    unfinishedTasksExecutor.restartUnfinishedTasks();
    //then
    Mockito.verify(tasksByStateDAO, Mockito.times(1)).findTasksByState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES);
  }

  private TaskInfo prepareTestTask() {
    TaskInfo taskInfo = TaskInfo.builder()
                                .id(1)
                                .topologyName("topoName")
                                .state(TaskState.PROCESSING_BY_REST_APPLICATION)
                                .stateDescription("info")
                                .sentTimestamp(new Date())
                                .startTimestamp(new Date())
                                .finishTimestamp(new Date())
                                .build();
    taskInfo.setDefinition(
        "{\"inputData\":{\"DATASET_URLS\":[\"http://195.216.97.81/api/data-providers/topologiesTestProvider/data-sets/DEREFERENCE_DATASET\"]},\"parameters\":{\"REPRESENTATION_NAME\":\"derefernce_rep\",\"AUTHORIZATION_HEADER\":\"Basic bWV0aXNfdGVzdDoxUmtaQnVWZg==\"},\"outputRevision\":null,\"taskId\":-2054267154868584315,\"taskName\":\"\",\"harvestingDetails\":null}");
    return taskInfo;
  }

  private TaskByTaskState prepareTestTaskByTaskState() {
    return TaskByTaskState.builder()
                          .id(1L)
                          .topologyName("topoName")
                          .state(TaskState.PROCESSING_BY_REST_APPLICATION)
                          .applicationId("exampleAppIdentifier")
                          .startTime(GregorianCalendar.getInstance().getTime())
                          .topicName("topicName")
                          .build();
  }

  private TaskByTaskState prepareTestTaskByTaskStateForAnotherMachine() {
    TaskByTaskState result = prepareTestTaskByTaskState();
    result.setApplicationId("exampleAppIdentifierForAnotherMachine");
    return result;
  }
}
