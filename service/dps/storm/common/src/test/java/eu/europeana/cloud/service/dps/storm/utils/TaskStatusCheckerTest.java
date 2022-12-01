package eu.europeana.cloud.service.dps.storm.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CassandraTaskInfoDAO.class)
@PowerMockIgnore({"javax.management.*", "org.apache.logging.log4j.*", "com.sun.org.apache.xerces.*",
    "eu.europeana.cloud.test.CassandraTestInstance"})
public class TaskStatusCheckerTest {

  private static TaskStatusChecker taskStatusChecker;
  private static CassandraTaskInfoDAO taskInfoDAO;
  private static CassandraConnectionProvider cassandraConnectionProvider;
  private final static long TASK_ID = 1234;
  private final static long TASK_ID2 = 123456;
  private final static int STATUS_CHECKER_CACHE_CHECK_INTERVAL = 100;
  private final static double SAFETY_THRESHOLD_PERCENTAGE = 0.1;

  @Before
  public void init() throws Exception {
    cassandraConnectionProvider = mock(CassandraConnectionProvider.class);
    taskInfoDAO = Mockito.mock(CassandraTaskInfoDAO.class);
    PowerMockito.mockStatic(CassandraTaskInfoDAO.class);
    when(CassandraTaskInfoDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(taskInfoDAO);
    taskStatusChecker = new TaskStatusChecker(taskInfoDAO, STATUS_CHECKER_CACHE_CHECK_INTERVAL);

  }

  @Test
  public void testExecutionWithMultipleTasks() throws Exception {
    when(taskInfoDAO.isDroppedTask(TASK_ID)).thenReturn(false, false, false, true, true);
    when(taskInfoDAO.isDroppedTask(TASK_ID2)).thenReturn(false, false, true);
    boolean task1killedFlag = false;
    boolean task2killedFlag = false;

    for (int i = 0; i < 8; i++) {
      if (i < 4) {
        assertFalse(task1killedFlag);
      }
      if (i < 3) {
        assertFalse(task2killedFlag);
      }
      task1killedFlag = taskStatusChecker.hasDroppedStatus(TASK_ID);
      if (i < 5) {
        task2killedFlag = taskStatusChecker.hasDroppedStatus(TASK_ID2);
      }
      Thread.sleep((long) (STATUS_CHECKER_CACHE_CHECK_INTERVAL * (1 + SAFETY_THRESHOLD_PERCENTAGE)));
    }
    verify(taskInfoDAO, times(8)).isDroppedTask(TASK_ID);
    verify(taskInfoDAO, times(5)).isDroppedTask(TASK_ID2);
    assertTrue(task1killedFlag);
    assertTrue(task2killedFlag);
    Thread.sleep((long) (STATUS_CHECKER_CACHE_CHECK_INTERVAL * (1 + SAFETY_THRESHOLD_PERCENTAGE)));
    verifyNoMoreInteractions(taskInfoDAO);

  }

  @Test
  public void TaskStatusCheckerShouldOnlyBeInitialedOnce() {
    TaskStatusChecker firstTaskStatusChecker = TaskStatusChecker.getTaskStatusChecker(cassandraConnectionProvider);
    TaskStatusChecker secondTaskStatusChecker = TaskStatusChecker.getTaskStatusChecker(cassandraConnectionProvider);
    Assert.assertEquals(firstTaskStatusChecker, secondTaskStatusChecker);
  }
}