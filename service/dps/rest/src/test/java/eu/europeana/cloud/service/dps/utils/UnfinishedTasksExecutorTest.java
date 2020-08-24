package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.model.dps.TaskTopicInfo;
import eu.europeana.cloud.service.dps.config.UnfinishedTasksContext;
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

import static org.mockito.Mockito.when;

@ContextConfiguration(classes={UnfinishedTasksContext.class, UnfinishedTasksExecutor.class})
@RunWith(SpringRunner.class)
public class UnfinishedTasksExecutorTest {

    @Autowired
    private TasksByStateDAO cassandraTasksDAO;

    @Autowired
    private UnfinishedTasksExecutor unfinishedTasksExecutor;

    @Test
    public void shouldNotStartExecutionForEmptyTasksList() {
        List<TaskTopicInfo> unfinishedTasks = new ArrayList<>();
        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(Mockito.any(TaskState.class))).thenReturn(unfinishedTasks);
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }

    @Test
    public void shouldStartExecutionForOneTasks() {
        List<TaskTopicInfo> unfinishedTasks = new ArrayList<>();
        TaskTopicInfo taskInfo = createTask();
        taskInfo.setOwnerId("exampleAppIdentifier");
        unfinishedTasks.add(taskInfo);

        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION)).thenReturn(unfinishedTasks);
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }



    @Test
    public void shouldStartExecutionForTasksThatBelongsToGivenMachine() {
        List<TaskTopicInfo> unfinishedTasks = new ArrayList<>();
        TaskTopicInfo taskInfo = createTask();
        taskInfo.setOwnerId("exampleAppIdentifier");
        unfinishedTasks.add(taskInfo);
        //
        TaskTopicInfo taskInfoForAnotherMachine = createTask();
        taskInfoForAnotherMachine.setOwnerId("exampleAppIdentifierForAnotherMachine");
        unfinishedTasks.add(taskInfoForAnotherMachine);

        Mockito.reset(cassandraTasksDAO);
        when(cassandraTasksDAO.findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION)).thenReturn(unfinishedTasks);
        unfinishedTasksExecutor.reRunUnfinishedTasks();
        Mockito.verify(cassandraTasksDAO, Mockito.times(1)).findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }

    private TaskTopicInfo createTask() {
        TaskTopicInfo info = new TaskTopicInfo();
        info.setId(1L);
        info.setState(TaskState.PROCESSING_BY_REST_APPLICATION.toString());
        info.setTopologyName("topoName");
        return info;
    }
}
