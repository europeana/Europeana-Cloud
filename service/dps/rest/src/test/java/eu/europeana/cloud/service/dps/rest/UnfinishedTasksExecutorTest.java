package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.when;

@ContextConfiguration(classes={UnfinishedTasksContext.class,UnfinishedTasksExecutor.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class UnfinishedTasksExecutorTest {

    @Autowired
    private TasksByStateDAO cassandraTasksDAO;

    @Autowired
    private UnfinishedTasksExecutor unfinishedTasksExecutor;

    @Test
    public void shouldNotStartExecutionForEmptyTasksList() {
        List<TaskInfo> unfinishedTasks = new ArrayList<>();
        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(Mockito.any(TaskState.class))).thenReturn(unfinishedTasks);
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }

    @Test
    public void shouldStartExecutionForOneTasks() {
        List<TaskInfo> unfinishedTasks = new ArrayList<>();
        TaskInfo taskInfo = new TaskInfo(1L, "topoName", TaskState.PROCESSING_BY_REST_APPLICATION, "info",
                new Date(), new Date(), new Date());
        taskInfo.setOwnerId("exampleAppIdentifier");
        unfinishedTasks.add(taskInfo);

        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION)).thenReturn(unfinishedTasks);
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }

    @Test
    public void shouldStartExecutionForTasksThatBelongsToGivenMachine() {
        List<TaskInfo> unfinishedTasks = new ArrayList<>();
        TaskInfo taskInfo = new TaskInfo(1L, "topoName", TaskState.PROCESSING_BY_REST_APPLICATION, "info",
                new Date(), new Date(), new Date());
        taskInfo.setOwnerId("exampleAppIdentifier");
        unfinishedTasks.add(taskInfo);
        //
        TaskInfo taskInfoForAnotherMachine = new TaskInfo(1L, "topoName", TaskState.PROCESSING_BY_REST_APPLICATION, "info",
                new Date(), new Date(), new Date());
        taskInfoForAnotherMachine.setOwnerId("exampleAppIdentifierForAnotherMachine");
        unfinishedTasks.add(taskInfoForAnotherMachine);

        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION)).thenReturn(unfinishedTasks);
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }

}
