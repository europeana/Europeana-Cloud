package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

public class RetryableMethodExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryableMethodExecutor.class);

    private static final int DEFAULT_DB_RETRIES = 4;

    private static final int DEFAULT_REST_RETRIES = 8;

    private static final int SLEEP_TIME = 5000;

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

    public static <V, E extends Throwable> V execute(String errorMessage, int maxAttempts, int sleepTimeBetweenRetriesMs, GenericCallable<V, E> callable) throws E {
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                if (--maxAttempts > 0) {
                    LOGGER.warn(errorMessage + " Retries Left {} ", maxAttempts, e);
                    waitForSpecificTime(sleepTimeBetweenRetriesMs);
                } else {
                    LOGGER.error(errorMessage);
                    throw (E) e;
                }
            }
        }
    }

    private static void waitForSpecificTime(int milliSecond) {
        try {
            Thread.sleep(milliSecond);
        } catch (InterruptedException e) {
            throw new RuntimeException("Stop waiting for retry because interrupted flag set on Thread!", e);
        }
    }

    public static <T> T createRetryProxy(T target) {
        Enhancer enhancer = new Enhancer();
        enhancer.setClassLoader(target.getClass().getClassLoader());
        enhancer.setSuperclass(target.getClass());
        enhancer.setCallback(
                (MethodInterceptor) (obj, method, args, methodProxy) -> retryFromAnnotation(target, method, args)
        );

        return (T) enhancer.create();
    }

    static <T> Object retryFromAnnotation(T target, Method method, Object[] args) throws Throwable {
        Retryable retryAnnotation = method.getAnnotation(Retryable.class);
        if (retryAnnotation != null) {
            String errorMessage = retryAnnotation.errorMessage().isEmpty() ?
                    String.format("Error while invoking method '%s' with args: %s", method, Arrays.toString(args)) :
                    retryAnnotation.errorMessage();

            return execute(errorMessage, retryAnnotation.maxAttempts(), retryAnnotation.delay(),
                    () -> invokeWithThrowingOriginalException(target, method, args)
            );
        } else {
            return invokeWithThrowingOriginalException(target, method, args);
        }
    }

    private static <T> Object invokeWithThrowingOriginalException(T target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (ReflectiveOperationException e) {
            throw e.getCause();
        }
    }


    public interface GenericRunnable<E extends Exception> {
        void run() throws E;
    }

    public interface GenericCallable<V, E extends Throwable> {
        V call() throws E;
    }
}
