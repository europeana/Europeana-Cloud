package eu.europeana.cloud.persisted;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import java.util.Optional;
import org.junit.Before;
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

  private final int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT)
                                           .orElse(RemoverImpl.DEFAULT_RETRIES);
  private static final long TASK_ID = 1234;



  @Before
  public void init() {
    MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    removerImpl = new RemoverImpl(subTaskInfoDAO, taskErrorDAO, statisticsService);
  }

  @Test
  public void shouldSuccessfullyRemoveNotifications() {
    doNothing().when(subTaskInfoDAO).removeNotifications(TASK_ID);
    removerImpl.removeNotifications(TASK_ID);
    verify(subTaskInfoDAO, times(1)).removeNotifications((TASK_ID));
  }

  @Test(expected = Exception.class)
  public void shouldRetryBeforeFailing() {
    doThrow(Exception.class).when(subTaskInfoDAO).removeNotifications(TASK_ID);
    removerImpl.removeNotifications(TASK_ID);
    verify(subTaskInfoDAO, times(attemptCount)).removeNotifications((TASK_ID));
  }


  @Test
  public void shouldSuccessfullyRemoveErrors() {
    doNothing().when(taskErrorDAO).removeErrors(TASK_ID);
    removerImpl.removeErrorReports(TASK_ID);
    verify(taskErrorDAO, times(1)).removeErrors((TASK_ID));
  }

  @Test(expected = Exception.class)
  public void shouldRetryBeforeFailingWhileRemovingErrorReports() {
    doThrow(Exception.class).when(taskErrorDAO).removeErrors(TASK_ID);
    removerImpl.removeErrorReports(TASK_ID);
    verify(taskErrorDAO, times(attemptCount)).removeErrors((TASK_ID));
  }

  @Test
  public void shouldSuccessfullyRemoveStatistics() {
    doNothing().when(statisticsService).removeStatistics(TASK_ID);
    removerImpl.removeStatistics(TASK_ID);
    verify(statisticsService, times(1)).removeStatistics((TASK_ID));
  }

  @Test(expected = Exception.class)
  public void shouldRetryBeforeFailingWhileRemovingStatistics() {
    doThrow(Exception.class).when(statisticsService).removeStatistics(TASK_ID);
    removerImpl.removeStatistics(TASK_ID);
    verify(statisticsService, times(attemptCount)).removeStatistics((TASK_ID));

  }
}