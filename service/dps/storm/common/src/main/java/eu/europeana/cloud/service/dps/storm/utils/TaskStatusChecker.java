package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tarek on 4/9/2018.
 */
public class TaskStatusChecker {

    public static final int CHECKING_INTERVAL_IN_SECONDS = 5;
    public static final int CONCURRENCY_LEVEL = 1000;
    public static final int SIZE = 100;

    private static TaskStatusChecker instance;
    private CassandraTaskInfoDAO taskDAO;

    private static volatile LoadingCache<Long, Boolean> cache;

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusChecker.class);

    private TaskStatusChecker(CassandraConnectionProvider cassandraConnectionProvider) {
        cache = CacheBuilder.newBuilder().refreshAfterWrite(CHECKING_INTERVAL_IN_SECONDS, TimeUnit.SECONDS).concurrencyLevel(CONCURRENCY_LEVEL).maximumSize(SIZE).softValues()
                .build(new CacheLoader<Long, Boolean>() {
                    public Boolean load(Long taskId) throws ExecutionException, TaskInfoDoesNotExistException {
                        return isTaskKilled(taskId);
                    }
                });
        this.taskDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
    }

    private TaskStatusChecker(CassandraTaskInfoDAO taskDAO) {
        cache = CacheBuilder.newBuilder().refreshAfterWrite(CHECKING_INTERVAL_IN_SECONDS, TimeUnit.SECONDS).concurrencyLevel(CONCURRENCY_LEVEL).maximumSize(SIZE).softValues()
                .build(new CacheLoader<Long, Boolean>() {
                    public Boolean load(Long taskId) throws ExecutionException, TaskInfoDoesNotExistException {
                        return isTaskKilled(taskId);
                    }
                });
        this.taskDAO = taskDAO;
    }

    public static synchronized TaskStatusChecker getTaskStatusChecker() {
        if (instance == null) {
            throw new IllegalStateException("TaskStatusChecker has not been initialized!. Please initialize it first");
        }
        return instance;
    }

    public static synchronized void init(CassandraConnectionProvider cassandraConnectionProvider) {
        if (instance == null) {
            instance = new TaskStatusChecker(cassandraConnectionProvider);
        }
    }

    public static synchronized void init(CassandraTaskInfoDAO taskDAO) {
        if (instance == null) {
            instance = new TaskStatusChecker(taskDAO);
        }
    }


    public boolean hasKillFlag(long taskId) {
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
    private Boolean isTaskKilled(long taskId) throws TaskInfoDoesNotExistException {
        boolean isKilled = false;
        LOGGER.info("Checking the task status for the task id from backend: {}" , taskId);
        if (taskDAO.hasKillFlag(taskId)) {
            isKilled = true;
        }
        return isKilled;
    }
}

