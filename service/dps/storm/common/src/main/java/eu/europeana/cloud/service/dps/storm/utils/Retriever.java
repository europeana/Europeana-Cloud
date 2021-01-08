package eu.europeana.cloud.service.dps.storm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Retriever {
    private final static Logger LOGGER = LoggerFactory.getLogger(Retriever.class);

    private static final int DEFAULT_CASSANDRA_RETRIES = 3;

    private static final int DEFAULT_ECLOUD_RETRIES = 7;

    private static final int SLEEP_TIME = 5000;

    public static <E extends Exception> void retryOnCassandraOnError(String errorMessage, GenericRunnable<E> runnable) throws E {
        Retriever.retryOnError(errorMessage, DEFAULT_CASSANDRA_RETRIES, SLEEP_TIME, () -> {
                    runnable.run();
                    return null;
                }
        );
    }

    public static <V,E extends Exception> V retryOnCassandraOnError(String errorMessage, GenericCallable<V, E> callable) throws E {
        return retryOnError(errorMessage, DEFAULT_CASSANDRA_RETRIES, SLEEP_TIME, callable);
    }

    public static <E extends Exception> void retryOnEcloudOnError(String errorMessage, GenericRunnable<E> runnable) throws E {
        Retriever.retryOnError(errorMessage, DEFAULT_ECLOUD_RETRIES, SLEEP_TIME, () -> {
                    runnable.run();
                    return null;
                }
        );
    }

    public static <V,E extends Exception> V retryOnEcloudOnError(String errorMessage, GenericCallable<V, E> callable) throws E {
        return retryOnError(errorMessage, DEFAULT_ECLOUD_RETRIES, SLEEP_TIME, callable);
    }

    public static <V, E extends Exception> V retryOnError(String errorMessage, int retryCount, int sleepTimeBetweenRetriesMs, GenericCallable<V, E> callable) throws E {
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (retryCount-- > 0) {
                    LOGGER.warn(errorMessage +" Retries Left {} ", retryCount, e);
                    waitForSpecificTime(sleepTimeBetweenRetriesMs);
                } else {
                    LOGGER.error(errorMessage);
                    throw (E) e;
                }
            }
        }
    }

    public static void waitForSpecificTime(int milliSecond) {
        try {
            Thread.sleep(milliSecond);
        } catch (InterruptedException e) {
            throw new RuntimeException("Stop waiting for retry because interrupted flag set on Thread!",e);
        }
    }

    public interface GenericRunnable<E extends Exception> {
        void run() throws E;
    }

    public interface GenericCallable<V, E extends Exception> {
        V call() throws E;
    }
}
