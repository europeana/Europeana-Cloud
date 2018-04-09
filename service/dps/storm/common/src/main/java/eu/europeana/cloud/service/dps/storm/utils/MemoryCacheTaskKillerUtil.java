package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tarek on 4/9/2018.
 */
public class MemoryCacheTaskKillerUtil {
    public static final int CHECKING_INTERVAL = 5000;
    public static final int EXPIRATION_IN_MINUTES = 60;
    public static final int CONCURRENCY_LEVEL = 10000;
    public static final int SIZE = 100;
    private static MemoryCacheTaskKillerUtil instance;
    private CassandraTaskInfoDAO taskDAO;
    private static volatile Cache<Long, Boolean> cache;
    private volatile long lastCheck = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryCacheTaskKillerUtil.class);

    private MemoryCacheTaskKillerUtil(int concurrencyLevel, int expiration, int size, CassandraConnectionProvider cassandraConnectionProvider) {

        cache = CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel).maximumSize(size).softValues()
                .expireAfterWrite(expiration, TimeUnit.MINUTES).build();
        this.taskDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
    }

    public static synchronized MemoryCacheTaskKillerUtil getMemoryCacheTaskKillerUtil(CassandraConnectionProvider cassandraConnectionProvider) {
        if (instance == null) {
            instance = new MemoryCacheTaskKillerUtil(CONCURRENCY_LEVEL, EXPIRATION_IN_MINUTES, SIZE, cassandraConnectionProvider);
        }
        return instance;
    }


    public boolean hasKillFlag(long taskId) {
        ConcurrentMap<Long, Boolean> map = cache.asMap();
        if (map.get(taskId) == null && lastCheck + CHECKING_INTERVAL < System.currentTimeMillis()) {
            synchronized (instance) {
                if (lastCheck + CHECKING_INTERVAL < System.currentTimeMillis()) {
                    LOGGER.info("Checking the cancellation from the backend database");
                    lastCheck = System.currentTimeMillis();
                    if (taskDAO.hasKillFlag(taskId))
                        cache.put(taskId, true);
                }
            }

        }

        if (map.get(taskId) == null)
            return false;
        return map.get(taskId);
    }
}

