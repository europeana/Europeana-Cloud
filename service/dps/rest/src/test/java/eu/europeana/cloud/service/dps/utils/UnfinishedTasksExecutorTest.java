package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.config.UnfinishedTasksContext;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;

@ContextConfiguration(classes={UnfinishedTasksContext.class, UnfinishedTasksExecutor.class})
@RunWith(SpringRunner.class)
public class UnfinishedTasksExecutorTest {

    @Autowired
    private TasksByStateDAO cassandraTasksDAO;

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

        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(Mockito.any(List.class))).thenReturn(unfinishedTasks);
        //when
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        //then
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES);
    }

    @Test
    public void shouldStartExecutionForOneTasks() throws TaskInfoDoesNotExistException {
        //given
        List<TaskInfo> unfinishedTasks = new ArrayList<>();
        TaskInfo taskInfo = prepareTestTask();
        unfinishedTasks.add(taskInfo);

        Mockito.reset(cassandraTasksDAO);
        Mockito.reset(taskSubmitterFactory);
        when(cassandraTasksDAO.findTasksInGivenState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES)).thenReturn(unfinishedTasks);
        when(cassandraTaskInfoDAO.findById(1L)).thenReturn(Optional.of(taskInfo));
        when(taskSubmitterFactory.provideTaskSubmitter(Mockito.any(SubmitTaskParameters.class))).thenReturn(Mockito.mock(TaskSubmitter.class));
        //when
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        //then
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES);
        Mockito.verify(taskSubmitterFactory, Mockito.times(1)).provideTaskSubmitter(Mockito.any(SubmitTaskParameters.class));
    }



    @Test
    public void shouldStartExecutionForTasksThatBelongsToGivenMachine() throws TaskInfoDoesNotExistException {
        //given
        List<TaskInfo> unfinishedTasks = new ArrayList<>();
        TaskInfo taskInfo = prepareTestTask();
        unfinishedTasks.add(taskInfo);
        unfinishedTasks.add(prepareTestTaskForAnotherMachine());

        Mockito.reset(cassandraTasksDAO);
        Mockito.reset(taskSubmitterFactory);
        when(cassandraTasksDAO.findTasksInGivenState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES)).thenReturn(unfinishedTasks);
        when(cassandraTaskInfoDAO.findById(1L)).thenReturn(Optional.of(taskInfo));
        when(taskSubmitterFactory.provideTaskSubmitter(Mockito.any(SubmitTaskParameters.class))).thenReturn(Mockito.mock(TaskSubmitter.class));
        //when
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        //then
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(UnfinishedTasksExecutor.RESUMABLE_TASK_STATES);
        Mockito.verify(taskSubmitterFactory, Mockito.times(1)).provideTaskSubmitter(Mockito.any(SubmitTaskParameters.class));
    }

    private TaskInfo prepareTestTask(){
        TaskInfo taskInfo = new TaskInfo(1L, "topoName", TaskState.PROCESSING_BY_REST_APPLICATION, "info",
                new Date(), new Date(), new Date());
        taskInfo.setOwnerId("exampleAppIdentifier");
        taskInfo.setTaskDefinition("{\"inputData\":{\"DATASET_URLS\":[\"http://195.216.97.81/api/data-providers/topologiesTestProvider/data-sets/DEREFERENCE_DATASET\"]},\"parameters\":{\"REPRESENTATION_NAME\":\"derefernce_rep\",\"AUTHORIZATION_HEADER\":\"Basic bWV0aXNfdGVzdDoxUmtaQnVWZg==\"},\"outputRevision\":null,\"taskId\":-2054267154868584315,\"taskName\":\"\",\"harvestingDetails\":null}");
        return taskInfo;
    }

    private TaskInfo prepareTestTaskForAnotherMachine(){
        TaskInfo taskInfoForAnotherMachine = new TaskInfo(1L, "topoName", TaskState.PROCESSING_BY_REST_APPLICATION, "info",
                new Date(), new Date(), new Date());
        taskInfoForAnotherMachine.setOwnerId("exampleAppIdentifierForAnotherMachine");
        return taskInfoForAnotherMachine;
    }
}
