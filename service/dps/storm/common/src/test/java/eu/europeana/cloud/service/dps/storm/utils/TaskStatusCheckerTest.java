package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 4/9/2018.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CassandraTaskInfoDAO.class)
@PowerMockIgnore({"javax.management.*"})
public class TaskStatusCheckerTest {

    private static TaskStatusChecker taskStatusChecker;
    private static CassandraTaskInfoDAO taskInfoDAO;
    private static CassandraConnectionProvider cassandraConnectionProvider;
    private final static long TASK_ID = 1234;
    private final static long TASK_ID2 = 123456;

    @BeforeClass
    public static void init() throws Exception {
        cassandraConnectionProvider = mock(CassandraConnectionProvider.class);
        taskInfoDAO = Mockito.mock(CassandraTaskInfoDAO.class);
        PowerMockito.mockStatic(CassandraTaskInfoDAO.class);
        when(CassandraTaskInfoDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(taskInfoDAO);
        TaskStatusChecker.init(cassandraConnectionProvider);
        taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();

    }

    @Test
    public void testExecutionWithMultipleTasks() throws Exception {

        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false, false, false, true, true);
        when(taskInfoDAO.hasKillFlag(TASK_ID2)).thenReturn(false, false, true);
        boolean task1killedFlag = false;
        boolean task2killedFlag = false;

        for (int i = 0; i < 8; i++) {
            if (i < 4)
                assertFalse(task1killedFlag);
            if (i < 3)
                assertFalse(task2killedFlag);
            task1killedFlag = taskStatusChecker.hasKillFlag(TASK_ID);
            if (i < 5)
                task2killedFlag = taskStatusChecker.hasKillFlag(TASK_ID2);
            Thread.sleep(6000);
        }
        verify(taskInfoDAO, times(8)).hasKillFlag(eq(TASK_ID));
        verify(taskInfoDAO, times(5)).hasKillFlag(eq(TASK_ID2));
        assertTrue(task1killedFlag);
        assertTrue(task2killedFlag);
        Thread.sleep(20000);
        verifyNoMoreInteractions(taskInfoDAO);

    }

    @Test
    public void TaskStatusCheckerShouldOnlyBeInitialedOnce() {
        TaskStatusChecker.init(cassandraConnectionProvider);
        TaskStatusChecker firstTaskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
        TaskStatusChecker.init(cassandraConnectionProvider);
        TaskStatusChecker secondTaskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
        Assert.assertEquals(firstTaskStatusChecker, secondTaskStatusChecker);

    }

}