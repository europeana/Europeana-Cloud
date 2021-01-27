package eu.europeana.cloud.service.dps.storm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryableMethodExecutor {
    private final static Logger LOGGER = LoggerFactory.getLogger(RetryableMethodExecutor.class);

    private static final int DEFAULT_DB_RETRIES = 3;

    private static final int DEFAULT_REST_RETRIES = 7;

    private static final int SLEEP_TIME = 5000;

    public static <E extends Exception> void executeOnDb(String errorMessage, GenericRunnable<E> runnable) throws E {
        RetryableMethodExecutor.execute(errorMessage, DEFAULT_DB_RETRIES, SLEEP_TIME, () -> {
                    runnable.run();
                    return null;
                }
        );
    }

    public static <V, E extends Exception> V executeOnDb(String errorMessage, GenericCallable<V, E> callable) throws E {
        return execute(errorMessage, DEFAULT_DB_RETRIES, SLEEP_TIME, callable);
    }

    public static <E extends Exception> void executeOnRest(String errorMessage, GenericRunnable<E> runnable) throws E {
        RetryableMethodExecutor.execute(errorMessage, DEFAULT_REST_RETRIES, SLEEP_TIME, () -> {
                    runnable.run();
                    return null;
                }
        );
    }

    public static <V, E extends Exception> V executeOnRest(String errorMessage, GenericCallable<V, E> callable) throws E {
        return execute(errorMessage, DEFAULT_REST_RETRIES, SLEEP_TIME, callable);
    }

    public static <V, E extends Exception> V execute(String errorMessage, int retryCount, int sleepTimeBetweenRetriesMs, GenericCallable<V, E> callable) throws E {
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (retryCount-- > 0) {
                    LOGGER.warn(errorMessage + " Retries Left {} ", retryCount, e);
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
            throw new RuntimeException("Stop waiting for retry because interrupted flag set on Thread!", e);
        }
    }

    public interface GenericRunnable<E extends Exception> {
        void run() throws E;
    }

    public interface GenericCallable<V, E extends Exception> {
        V call() throws E;
    }
}
