package eu.europeana.cloud.persisted;

import eu.europeana.cloud.api.Remover;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;


/**
 * Created by Tarek on 4/16/2019.
 */
public class RemoverImpl implements Remover {

  public static final String LOG_REMOVAL_ERROR = "Error while removing the logs.";
  protected static final int DEFAULT_RETRIES = 5;
  protected static final int SLEEP_TIME = 3000;


  private final NotificationsDAO subTaskInfoDAO;
  private final CassandraTaskErrorsDAO taskErrorDAO;
  private final ValidationStatisticsServiceImpl statisticsService;


  public RemoverImpl(String hosts, int port, String keyspaceName, String userName, String password) {
    CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
        hosts, port, keyspaceName,
        userName, password);
    subTaskInfoDAO = NotificationsDAO.getInstance(cassandraConnectionProvider);
    taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
    statisticsService = ValidationStatisticsServiceImpl.getInstance(cassandraConnectionProvider);
  }

  RemoverImpl(NotificationsDAO subTaskInfoDAO, CassandraTaskErrorsDAO taskErrorDAO,
      ValidationStatisticsServiceImpl statisticsService) {
    this.subTaskInfoDAO = subTaskInfoDAO;
    this.taskErrorDAO = taskErrorDAO;
    this.statisticsService = statisticsService;
  }

  @Override
  public void removeNotifications(long taskId) {
    RetryableMethodExecutor
        .execute(LOG_REMOVAL_ERROR, DEFAULT_RETRIES, SLEEP_TIME, () -> {
          subTaskInfoDAO.removeNotifications(taskId);
          return null;
        });
  }

  @Override
  public void removeErrorReports(long taskId) {
    RetryableMethodExecutor
        .execute(LOG_REMOVAL_ERROR, DEFAULT_RETRIES, SLEEP_TIME, () -> {
          taskErrorDAO.removeErrors(taskId);
          return null;
        });
  }

  @Override
  public void removeStatistics(long taskId) {
    RetryableMethodExecutor
        .execute(LOG_REMOVAL_ERROR, DEFAULT_RETRIES, SLEEP_TIME, () -> {
          statisticsService.removeStatistics(taskId);
          return null;
        });

  }

}
