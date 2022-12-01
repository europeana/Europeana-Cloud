package eu.europeana.cloud.persisted;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.test.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RemoverImplTest {


  @Mock(name = "subTaskInfoDAO")
  private NotificationsDAO subTaskInfoDAO;


  @Mock(name = "taskErrorDAO")
  private CassandraTaskErrorsDAO taskErrorDAO;


  @Mock(name = "cassandraNodeStatisticsDAO")
  private ValidationStatisticsServiceImpl statisticsService;

  private RemoverImpl removerImpl;

  private static final long TASK_ID = 1234;

  @BeforeClass
  public static void initTest() {
    TestUtils.changeFieldValueForClass(RemoverImpl.class, "DEFAULT_RETRIES",
        TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES);
    TestUtils.changeFieldValueForClass(RemoverImpl.class, "SLEEP_TIME",
        TestUtils.DEFAULT_DELAY_BETWEEN_ATTEMPTS);
  }

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    removerImpl = new RemoverImpl(subTaskInfoDAO, taskErrorDAO, statisticsService);
  }

  @Test
  public void shouldSuccessfullyRemoveNotifications() {
    doNothing().when(subTaskInfoDAO).removeNotifications(eq(TASK_ID));
    removerImpl.removeNotifications(TASK_ID);
    verify(subTaskInfoDAO, times(1)).removeNotifications((eq(TASK_ID)));
  }

  @Test(expected = Exception.class)
  public void shouldRetryBeforeFailing() {
    doThrow(Exception.class).when(subTaskInfoDAO).removeNotifications(eq(TASK_ID));
    removerImpl.removeNotifications(TASK_ID);
    verify(subTaskInfoDAO, times(TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES)).removeNotifications((eq(TASK_ID)));
  }


  @Test
  public void shouldSuccessfullyRemoveErrors() {
    doNothing().when(taskErrorDAO).removeErrors(eq(TASK_ID));
    removerImpl.removeErrorReports(TASK_ID);
    verify(taskErrorDAO, times(1)).removeErrors((eq(TASK_ID)));
  }

  @Test(expected = Exception.class)
  public void shouldRetryBeforeFailingWhileRemovingErrorReports() {
    doThrow(Exception.class).when(taskErrorDAO).removeErrors(eq(TASK_ID));
    removerImpl.removeErrorReports(TASK_ID);
    verify(taskErrorDAO, times(TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES)).removeErrors((eq(TASK_ID)));
  }

  @Test
  public void shouldSuccessfullyRemoveStatistics() {
    doNothing().when(statisticsService).removeStatistics(eq(TASK_ID));
    removerImpl.removeStatistics(TASK_ID);
    verify(statisticsService, times(1)).removeStatistics((eq(TASK_ID)));
  }

  @Test(expected = Exception.class)
  public void shouldRetryBeforeFailingWhileRemovingStatistics() {
    doThrow(Exception.class).when(statisticsService).removeStatistics(eq(TASK_ID));
    removerImpl.removeStatistics(TASK_ID);
    verify(statisticsService, times(TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES)).removeStatistics((eq(TASK_ID)));

  }
}