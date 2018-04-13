package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;


/**
 * Created by Tarek on 4/9/2018.
 */
public class TaskStatusChecker {
    public static final int CHECKING_INTERVAL = 5000;
    public static final int CONCURRENCY_LEVEL = 1000;
    public static final int SIZE = 100;

    private static TaskStatusChecker instance;
    private CassandraTaskInfoDAO taskDAO;


    private static volatile Cache<Long, CacheItem> cache;

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusChecker.class);

    private TaskStatusChecker(CassandraConnectionProvider cassandraConnectionProvider) {

        cache = CacheBuilder.newBuilder().concurrencyLevel(CONCURRENCY_LEVEL).maximumSize(SIZE).softValues()
                .build();
        this.taskDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
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
        } else
            throw new IllegalStateException("TaskStatusChecker has already been initialized");
    }


    public boolean hasKillFlag(long taskId) {
        CacheItem cacheItem = getCacheItem(taskId);
        if (!cacheItem.hasKilledFlag && cacheItem.lastCheck + CHECKING_INTERVAL < System.currentTimeMillis()) {
            synchronized (instance) {
                if (cacheItem.lastCheck + CHECKING_INTERVAL < System.currentTimeMillis()) {
                    LOGGER.info("Checking the cancellation from the backend database for task id {}", taskId);
                    cacheItem.lastCheck = System.currentTimeMillis();
                    if (taskDAO.hasKillFlag(taskId)) {
                        cacheItem.hasKilledFlag = true;
                        cache.put(taskId, cacheItem);
                    }
                }
            }

        }
        return cacheItem.hasKilledFlag;
    }


    private CacheItem getCacheItem(long taskId) {
        ConcurrentMap<Long, CacheItem> map = cache.asMap();
        map.putIfAbsent(taskId, new CacheItem());
        return map.get(taskId);
    }

    private static class CacheItem {
        volatile boolean hasKilledFlag = false;
        volatile long lastCheck = 0;
    }
}

