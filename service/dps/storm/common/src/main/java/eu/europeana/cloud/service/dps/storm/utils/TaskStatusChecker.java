package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 4/9/2018.
 */
public class TaskStatusChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusChecker.class);

  public static final int CHECKING_INTERVAL_IN_MILLISECONDS = 5_000;
  public static final int CONCURRENCY_LEVEL = 1000;
  public static final int SIZE = 100;

  private static TaskStatusChecker instance;

  private LoadingCache<Long, Boolean> cache;

  private CassandraTaskInfoDAO taskDAO;

  private TaskStatusChecker(CassandraConnectionProvider cassandraConnectionProvider) {
    this(CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider), CHECKING_INTERVAL_IN_MILLISECONDS);
  }

  public TaskStatusChecker(CassandraTaskInfoDAO taskDAO) {
    this(taskDAO, CHECKING_INTERVAL_IN_MILLISECONDS);
  }

  protected TaskStatusChecker(CassandraTaskInfoDAO taskDAO, int checkingInterval) {
    cache = CacheBuilder.newBuilder()
                        .refreshAfterWrite(checkingInterval, TimeUnit.MILLISECONDS)
                        .concurrencyLevel(CONCURRENCY_LEVEL).maximumSize(SIZE).softValues()
                        .build(new CacheLoader<>() {
                          public Boolean load(Long taskId) throws TaskInfoDoesNotExistException {
                            return isDroppedTask(taskId);
                          }
                        });
    this.taskDAO = taskDAO;
  }

  public static synchronized TaskStatusChecker getTaskStatusChecker(CassandraConnectionProvider cassandraConnectionProvider) {
    if (instance == null) {
      instance = new TaskStatusChecker(cassandraConnectionProvider);
    }
    return instance;
  }

  public void checkNotDropped(DpsTask task) throws TaskDroppedException {
    if (hasDroppedStatus(task.getTaskId())) {
      throw new TaskDroppedException(task);
    }
  }


  public boolean hasDroppedStatus(long taskId) {
    try {
      return cache.get(taskId);
    } catch (ExecutionException e) {
      LOGGER.info(e.getMessage());
      return false;
    }
  }

  /*
     This method will only be executed if there is no VALUE for KEY taskId inside cache or if refresh method was triggered.
     In the current implementation it will be triggered every 5 seconds if it was queried.
   */
  private Boolean isDroppedTask(long taskId) throws TaskInfoDoesNotExistException {
    LOGGER.info("Checking the task status for the task id from backend: {}", taskId);
    return (taskDAO.isDroppedTask(taskId));
  }
}

