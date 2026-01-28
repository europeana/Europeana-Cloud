package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskStatusCheckerTest {

  private static final long TASK_ID = 1234;
  private static final long TASK_ID2 = 123456;
  private static final int STATUS_CHECKER_CACHE_CHECK_INTERVAL = 100;
  private static final double SAFETY_THRESHOLD_PERCENTAGE = 0.1;

  @Mock
  private CassandraConnectionProvider cassandraConnectionProvider;

  @Mock
  private CassandraTaskInfoDAO taskInfoDAO;

  private TaskStatusChecker taskStatusChecker;

  private MockedStatic<CassandraTaskInfoDAO> daoStaticMock;

  @BeforeEach
  void init() {
    daoStaticMock = mockStatic(CassandraTaskInfoDAO.class);
    daoStaticMock.when(() -> CassandraTaskInfoDAO.getInstance(any(CassandraConnectionProvider.class)))
            .thenReturn(taskInfoDAO);

    taskStatusChecker = new TaskStatusChecker(taskInfoDAO, STATUS_CHECKER_CACHE_CHECK_INTERVAL);
  }

  @Test
  void testExecutionWithMultipleTasks() throws Exception {
    when(taskInfoDAO.isDroppedTask(TASK_ID)).thenReturn(false, false, false, true, true);
    when(taskInfoDAO.isDroppedTask(TASK_ID2)).thenReturn(false, false, true);

    boolean task1killedFlag = false;
    boolean task2killedFlag = false;

    for (int i = 0; i < 8; i++) {
      if (i < 4) assertFalse(task1killedFlag);
      if (i < 3) assertFalse(task2killedFlag);

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
  void taskStatusCheckerShouldOnlyBeInitializedOnce() {
    TaskStatusChecker firstChecker = TaskStatusChecker.getTaskStatusChecker(cassandraConnectionProvider);
    TaskStatusChecker secondChecker = TaskStatusChecker.getTaskStatusChecker(cassandraConnectionProvider);
    assertEquals(firstChecker, secondChecker);
  }

  @AfterEach
  void tearDownStaticMock() {
    if (daoStaticMock != null) {
      daoStaticMock.close();
    }
  }
}
